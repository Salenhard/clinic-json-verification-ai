"""HealthController — liveness / readiness probes."""

from __future__ import annotations

from flask import Blueprint, jsonify

from config import settings

health_bp = Blueprint("health", __name__, url_prefix="/api")


@health_bp.get("/health")
def health():
    return jsonify({"status": "ok", "model": settings.gemini_model}), 200
