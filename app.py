import json
import uuid
import threading
import sqlite3
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
import os
import logging

from google import genai
from pipeline import (
    JsonPreprocessor, AnalysisStage, JsonValidator,
    CorrectionStage, FinalizationStage, configure_limiter
)
from pipeline.base import PipelineError

load_dotenv()

app = Flask(__name__)
CORS(app)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY не найден в .env")

client = genai.Client(api_key=GEMINI_API_KEY)

configure_limiter(int(os.getenv("RPM", 15)))

stages = [
    JsonPreprocessor(client),
    AnalysisStage(client),
    JsonValidator(client),
    CorrectionStage(client),
    FinalizationStage(client)
]

def init_db():
    conn = sqlite3.connect('tasks.db')
    conn.execute('''CREATE TABLE IF NOT EXISTS tasks (
        task_id TEXT PRIMARY KEY,
        status TEXT,
        progress INTEGER,
        message TEXT,
        result TEXT,
        created_at TEXT
    )''')
    conn.commit()
    conn.close()

def update_task(task_id, status, progress, message, result=None):
    conn = sqlite3.connect('tasks.db')
    result_str = json.dumps(result, ensure_ascii=False) if result else None
    conn.execute(
        "UPDATE tasks SET status=?, progress=?, message=?, result=? WHERE task_id=?",
        (status, progress, message, result_str, task_id)
    )
    conn.commit()
    conn.close()

def process_task(task_id: str, input_data: dict, recommendations: str = "", recommendations_file=None):
    context = {
        "input_data": input_data,
        "recommendations": recommendations,
        "recommendations_file": recommendations_file,
        "progress": 0,
        "message": "Задача запущена"
    }

    try:
        update_task(task_id, "processing", 5, "Запуск пайплайна")
        for stage in stages:
            context = stage.run(context)
        update_task(task_id, "completed", 100, "Верификация завершена", context["final_result"])

    except Exception as e:
        update_task(task_id, "error", 0, f"Ошибка: {str(e)}")

@app.route('/api/verify', methods=['POST'])
def verify():
    if request.files and 'recommendations_file' in request.files:
        data_str = request.form.get('data')
        input_data = json.loads(data_str) if data_str else {}
        recommendations_text = request.form.get('recommendations', '')
        rec_file = request.files['recommendations_file']
    else:
        payload = request.get_json()
        input_data = payload.get('data', {})
        recommendations_text = payload.get('recommendations', '')
        rec_file = None

    task_id = str(uuid.uuid4())

    conn = sqlite3.connect('tasks.db')
    conn.execute(
        "INSERT INTO tasks (task_id, status, progress, message, created_at) VALUES (?, 'pending', 0, 'Создано', ?)",
        (task_id, datetime.now().isoformat())
    )
    conn.commit()
    conn.close()

    threading.Thread(
        target=process_task,
        args=(task_id, input_data, recommendations_text, rec_file),
        daemon=True
    ).start()

    return jsonify({"task_id": task_id, "status": "pending", "message": "Верификация запущена"}), 202


@app.route('/api/status/<task_id>', methods=['GET'])
def get_status(task_id):
    conn = sqlite3.connect('tasks.db')
    row = conn.execute("SELECT status, progress, message, result FROM tasks WHERE task_id=?", (task_id,)).fetchone()
    conn.close()
    if not row:
        return jsonify({"error": "Задача не найдена"}), 404

    status, progress, message, result = row
    resp = {"task_id": task_id, "status": status, "progress": progress, "message": message}
    if result:
        resp["result"] = json.loads(result)
    return jsonify(resp)


if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000, debug=True)