"""Flask application factory.

Usage
-----
Development:
    flask --app app run --debug

Production (gunicorn):
    gunicorn "app:create_app()" -w 4 -k gthread --threads 4 -b 0.0.0.0:5000

Tests:
    app = create_app()
    client = app.test_client()
"""

from __future__ import annotations

import logging

from flask import Flask
from flask_cors import CORS
from google import genai

from config import settings
from controller import health_bp, create_verification_blueprint, ai_list_bp, create_task_blueprint
from repository import SQLiteTaskRepository
from service import VerificationService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)


def create_app() -> Flask:
    """Compose all layers and return a ready Flask application."""
    settings.validate()

    # ── Infrastructure ────────────────────────────────────────────────────────
    genai_client = genai.Client(api_key=settings.gemini_api_key)
    repository = SQLiteTaskRepository(db_path=settings.db_path)

    # ── Service ───────────────────────────────────────────────────────────────
    verification_service = VerificationService(
        repository=repository,
        genai_client=genai_client,
        settings=settings,
    )

    # ── Flask app ─────────────────────────────────────────────────────────────
    app = Flask(__name__)
    CORS(app)
    app.config["MAX_CONTENT_LENGTH"] = settings.max_content_length_bytes

    # ── Register blueprints ───────────────────────────────────────────────────
    app.register_blueprint(health_bp)
    app.register_blueprint(create_verification_blueprint(verification_service))
    app.register_blueprint(ai_list_bp)
    app.register_blueprint(create_task_blueprint(repository))
    print(app.url_map)
    return app
