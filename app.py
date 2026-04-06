"""Clinical JSON Validator — Flask REST API.

Endpoints
---------
POST /api/verify
    Body: multipart/form-data
      • data                 : JSON string or JSON file (field 'json_file')
      • recommendations      : (optional) plain text with clinical guidelines
      • recommendations_file : (optional) PDF file with clinical guidelines
      • model                : (optional) Gemini model name
    Response 202: { "task_id": "...", "status": "pending" }

GET /api/status/<task_id>
    Response: full task status + result when done

GET /api/result/<task_id>
    Response: result only (202 if still running, 200 if done)

GET /api/health
    Response: { "status": "ok" }
"""

import json
import logging
import os
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime

from dotenv import load_dotenv
from flask import Flask, request, jsonify
from flask_cors import CORS

from google import genai
from pipeline import (
    JsonPreprocessor, AnalysisStage, JsonValidator,
    CorrectionStage, FinalizationStage, configure_limiter,
)
from pipeline.base import PipelineError

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("app")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite-preview")
RPM = int(os.getenv("RPM", "15"))
DB_PATH = os.getenv("DB_PATH", "tasks.db")

if not GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY не задан в .env или переменных окружения")

_genai_client = genai.Client(api_key=GEMINI_API_KEY)
configure_limiter(RPM)

app = Flask(__name__)
CORS(app)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024

def init_db() -> None:
    with _db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id    TEXT PRIMARY KEY,
                status     TEXT NOT NULL DEFAULT 'pending',
                progress   INTEGER NOT NULL DEFAULT 0,
                message    TEXT,
                result     TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT
            )
        """)
        conn.execute("PRAGMA journal_mode=WAL")

@contextmanager
def _db():
    """Open a SQLite connection with WAL and row_factory."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def create_task(task_id: str) -> None:
    with _db() as conn:
        conn.execute(
            "INSERT INTO tasks (task_id, status, progress, message, created_at) VALUES (?, 'pending', 0, 'Создано', ?)",
            (task_id, datetime.now().isoformat()),
        )


def update_task(task_id: str, status: str, progress: int, message: str, result=None) -> None:
    result_str = json.dumps(result, ensure_ascii=False) if result is not None else None
    with _db() as conn:
        conn.execute(
            "UPDATE tasks SET status=?, progress=?, message=?, result=?, updated_at=? WHERE task_id=?",
            (status, progress, message, result_str, datetime.now().isoformat(), task_id),
        )


def get_task(task_id: str) -> dict | None:
    with _db() as conn:
        row = conn.execute(
            "SELECT task_id, status, progress, message, result, created_at, updated_at FROM tasks WHERE task_id=?",
            (task_id,),
        ).fetchone()
    if row is None:
        return None
    d = dict(row)
    if d.get("result"):
        d["result"] = json.loads(d["result"])
    return d

def _build_stages(model: str) -> list:
    """Create a fresh set of stage objects for one job (not shared between jobs)."""
    return [
        JsonPreprocessor(_genai_client, model=model, requests_per_minute=RPM),
        AnalysisStage(_genai_client, model=model, requests_per_minute=RPM),
        JsonValidator(_genai_client, model=model, requests_per_minute=RPM),
        CorrectionStage(_genai_client, model=model, requests_per_minute=RPM),
        FinalizationStage(_genai_client, model=model, requests_per_minute=RPM),
    ]


_STAGE_MESSAGES = [
    (5,  "Предобработка входных данных"),
    (25, "Анализ соответствия рекомендациям"),
    (55, "Структурная валидация"),
    (75, "Исправление и дополнение"),
    (95, "Финализация результата"),
]


def process_task(
    task_id: str,
    input_data,
    recommendations: str,
    recommendations_bytes,
    recommendations_filename,
    model: str,
) -> None:
    context = {
    "input_data": input_data,
    "recommendations": recommendations,
    "recommendations_bytes": recommendations_bytes,
    "recommendations_filename": recommendations_filename,
}
    stages = _build_stages(model)
    try:
        update_task(task_id, "processing", 5, "Запуск пайплайна")

        context = stages[0].run(context)

        max_iterations = 5
        target_score = 1.0

        for i in range(max_iterations):
            update_task(task_id, "processing", 20 +  (i / (max_iterations - 1)) * 80, f"Итерация {i+1}: анализ и исправление")

            # --- Stage 2
            context = stages[1].run(context)

            # --- Stage 3
            context = stages[2].run(context)

            # Stage 4
            context = stages[3].run(context)

            if "corrected_data" in context:
                context["input_data"]   = context["corrected_data"]
                context["original_data"] = context["corrected_data"]

            data = context.get("corrected_data")

            score = context.get("analysis", {}).get("completeness_score", 0.0)

            print(f"Iteration {i+1}, completeness: {score}")

            if score >= target_score:
                update_task(task_id, "processing", 80, "Достигнута достаточная полнота")
                break

        context = stages[4].run(context)

        update_task(
            task_id,
            "completed",
            100,
            "Верификация завершена",
            context.get("final_result")
        )

    except Exception as e:
        update_task(task_id, "error", 0, f"Ошибка: {str(e)}")

@app.get("/api/health")
def health():
    return jsonify({"status": "ok", "model": GEMINI_MODEL})


@app.post("/api/verify")
def verify():
    """Submit a new verification task."""
    model = GEMINI_MODEL
    
    json_file = request.files.get("json_file")
    if json_file is not None:
        try:
            input_data = json.load(json_file)
        except Exception as e:
            return jsonify({"error": f"Невалидный JSON-файл: {e}"}), 400
    elif request.content_type and "application/json" in request.content_type:
        payload = request.get_json(silent=True) or {}
        input_data = payload.get("data")
        if input_data is None:
            return jsonify({"error": "Поле 'data' обязательно при передаче JSON в теле запроса"}), 400
    else:
        data_str = request.form.get("data")
        if not data_str:
            return jsonify({"error": "Передайте JSON-документ в поле 'data' или 'json_file'"}), 400
        try:
            input_data = json.loads(data_str)
        except Exception as e:
            return jsonify({"error": f"Невалидный JSON в поле 'data': {e}"}), 400

    model = request.form.get("model") or GEMINI_MODEL

    recommendations_file = request.files.get("recommendations_file")
    recommendations_text = request.form.get("recommendations", "")

    recommendations_bytes = None
    recommendations_filename = None

    if recommendations_file:
        recommendations_bytes = recommendations_file.read()
        recommendations_filename = recommendations_file.filename

    if not recommendations_bytes and not recommendations_text.strip():
        return jsonify({
            "error": "Передайте PDF-файл в 'recommendations_file' или текст в 'recommendations'"
        }), 400

    # ── Create task and start thread ───────────────────────────────────────────
    task_id = str(uuid.uuid4())
    create_task(task_id)

    threading.Thread(
        target=process_task,
        args=(
            task_id,
            input_data,
            recommendations_text,
            recommendations_bytes,
            recommendations_filename,
            model
        ),
        daemon=True,
        name=f"task-{task_id[:8]}",
    ).start()

    logger.info("Task %s submitted (model=%s)", task_id, model)
    return jsonify({"task_id": task_id, "status": "pending", "message": "Верификация запущена"}), 202


@app.get("/api/status/<task_id>")
def get_status(task_id: str):
    """Return full task status."""
    task = get_task(task_id)
    if task is None:
        return jsonify({"error": "Задача не найдена"}), 404
    return jsonify(task)


@app.get("/api/result/<task_id>")
def get_result(task_id: str):
    """Return only the result (202 while running, 200 when done, 500 on error)."""
    task = get_task(task_id)
    if task is None:
        return jsonify({"error": "Задача не найдена"}), 404

    if task["status"] == "error":
        return jsonify({"error": task.get("message")}), 500
    if task["status"] != "completed":
        return jsonify({"status": task["status"], "progress": task["progress"]}), 202

    return jsonify(task["result"]), 200


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000, debug=False)
