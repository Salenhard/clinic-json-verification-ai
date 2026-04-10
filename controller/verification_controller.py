"""VerificationController — Flask Blueprint with zero business logic.

Each route method does exactly three things:
  1. Parse / validate HTTP input.
  2. Call the service.
  3. Serialize the result back to JSON.

Nothing else belongs here.
"""

from __future__ import annotations

import json
import logging

from flask import Blueprint, jsonify, request

from service import VerificationService, VerificationRequest
from repository import TaskStatus

logger = logging.getLogger(__name__)

verification_bp = Blueprint("verification", __name__, url_prefix="/api")


def create_verification_blueprint(service: VerificationService) -> Blueprint:
    """Factory that binds a service instance to the blueprint's routes.

    Using a factory instead of a global reference keeps the blueprint
    testable and free of import-time side-effects.
    """

    @verification_bp.post("/verify")
    def verify():
        """Submit a new verification task.

        Accepts multipart/form-data OR application/json.
        """
        # ── Parse input JSON ──────────────────────────────────────────────────
        json_file = request.files.get("json_file")

        if json_file is not None:
            try:
                input_data = json.load(json_file)
            except Exception as exc:
                return jsonify({"error": f"Невалидный JSON-файл: {exc}"}), 400

        elif request.is_json:
            payload = request.get_json(silent=True) or {}
            input_data = payload.get("data")
            if input_data is None:
                return jsonify({"error": "Поле 'data' обязательно при передаче JSON в теле запроса"}), 400

        else:
            data_str = request.form.get("data")
            if not data_str:
                return jsonify({"error": "Передайте JSON в поле 'data' или файл в 'json_file'"}), 400
            try:
                input_data = json.loads(data_str)
            except Exception as exc:
                return jsonify({"error": f"Невалидный JSON в поле 'data': {exc}"}), 400

        # ── Parse recommendations ─────────────────────────────────────────────
        rec_file = request.files.get("recommendations_file")
        rec_text = request.form.get("recommendations", "")
        rec_bytes: bytes | None = rec_file.read() if rec_file else None
        rec_filename: str | None = rec_file.filename if rec_file else None

        if not rec_bytes and not rec_text.strip():
            return jsonify({
                "error": "Передайте PDF в 'recommendations_file' или текст в 'recommendations'"
            }), 400

        # ── Build request & submit ────────────────────────────────────────────
        req = VerificationRequest(
            input_data=input_data,
            recommendations=rec_text,
            recommendations_bytes=rec_bytes,
            recommendations_filename=rec_filename,
            llm_provider=request.form.get("llm_provider") or None,
            model=request.form.get("model") or None,
            api_key=request.form.get("api_key") or None,
            chunk_size=int(request.form.get("chunk_size")) or 12_000,
            overlap=int(request.form.get("overlap")) or 400,
            requests_per_minute=int(request.form.get("requests_per_minute")) or 15,
            target_score=float(request.form.get("target_score")) or 0.9,
            max_iterations=int(request.form.get("max_iterations")) or 3,
        )

        task_id = service.submit(req)
        return jsonify({"task_id": task_id, "status": "pending", "message": "Верификация запущена"}), 202

    @verification_bp.get("/status/<task_id>")
    def get_status(task_id: str):
        """Return full task record."""
        task = service.get_task(task_id)
        if task is None:
            return jsonify({"error": "Задача не найдена"}), 404
        return jsonify(task.to_dict()), 200

    @verification_bp.get("/result/<task_id>")
    def get_result(task_id: str):
        """Return only the result payload.

        HTTP semantics:
          202 — task still running
          200 — task completed successfully
          500 — task finished with an error
        """
        task = service.get_task(task_id)
        if task is None:
            return jsonify({"error": "Задача не найдена"}), 404

        if task.status == TaskStatus.ERROR:
            return jsonify({"error": task.message}), 500
        if task.status != TaskStatus.COMPLETED:
            return jsonify({"status": task.status.value, "progress": task.progress}), 202

        return jsonify(task.result), 200

    return verification_bp
