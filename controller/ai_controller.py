"""HealthController — liveness / readiness probes."""

from __future__ import annotations

from flask import Blueprint, jsonify

from config import settings

ai_list_bp = Blueprint("ai-list", __name__, url_prefix="/api")


@ai_list_bp.get("/ai-list")
def ai_list():
    return jsonify({[
        {"id": 1, "model": "gemini"},
        {"id": 2, "model": "claude"},
        {"id": 3, "model": "deepseek"},
        {"id": 4, "model": "grok"},
        {"id": 5, "model": "openai"},
        {"id": 6, "model": "gpt"},
        ]}), 200
