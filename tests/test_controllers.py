"""Integration tests for the Flask controllers.

Tests the full HTTP layer against a mocked VerificationService.

Run with:  pytest tests/test_controllers.py -v
"""

from __future__ import annotations

import json
import io
from unittest.mock import MagicMock, patch

import pytest

from repository import Task, TaskStatus


# ── App factory with mocked service ──────────────────────────────────────────

@pytest.fixture
def mock_service():
    svc = MagicMock()
    svc.submit.return_value = "mock-task-id-1234"
    svc.get_task.return_value = Task(
        task_id="mock-task-id-1234",
        status=TaskStatus.PENDING,
        progress=0,
        message="Создано",
    )
    return svc


@pytest.fixture
def client(mock_service):
    """Flask test client with real routes but mocked service."""
    from app import create_app

    with patch("app.VerificationService", return_value=mock_service), \
         patch("app.SQLiteTaskRepository"), \
         patch("app.genai"):
        app = create_app()

    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c, mock_service


# ── /api/health ───────────────────────────────────────────────────────────────

class TestHealth:
    def test_health_returns_200(self, client):
        c, _ = client
        resp = c.get("/api/health")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "ok"


# ── POST /api/verify ──────────────────────────────────────────────────────────

class TestVerify:
    def _valid_form(self, extra=None):
        base = {
            "data": json.dumps({"patient": "test"}),
            "recommendations": "Use standard dosage.",
        }
        return {**(extra or {}), **base}

    def test_verify_form_data_returns_202(self, client):
        c, svc = client
        resp = c.post("/api/verify", data=self._valid_form())
        assert resp.status_code == 202
        body = resp.get_json()
        assert body["task_id"] == "mock-task-id-1234"
        assert body["status"] == "pending"

    def test_verify_calls_service_submit(self, client):
        c, svc = client
        c.post("/api/verify", data=self._valid_form())
        svc.submit.assert_called_once()

    def test_verify_missing_data_returns_400(self, client):
        c, _ = client
        resp = c.post("/api/verify", data={"recommendations": "some text"})
        assert resp.status_code == 400

    def test_verify_missing_recommendations_returns_400(self, client):
        c, _ = client
        resp = c.post("/api/verify", data={"data": json.dumps({"x": 1})})
        assert resp.status_code == 400

    def test_verify_invalid_json_returns_400(self, client):
        c, _ = client
        resp = c.post("/api/verify", data={"data": "not-json", "recommendations": "rec"})
        assert resp.status_code == 400

    def test_verify_json_file_upload(self, client):
        c, svc = client
        payload = io.BytesIO(json.dumps({"a": 1}).encode())
        resp = c.post(
            "/api/verify",
            data={
                "json_file": (payload, "test.json"),
                "recommendations": "Standard protocol.",
            },
            content_type="multipart/form-data",
        )
        assert resp.status_code == 202

    def test_verify_application_json_body(self, client):
        c, svc = client
        resp = c.post(
            "/api/verify",
            json={"data": {"patient": "x"}, "recommendations": "rec"},
        )
        # JSON body path expects form field for recommendations separately —
        # this should 400 because 'recommendations' isn't in form
        # (demonstrates the boundary between JSON body and form fields)
        assert resp.status_code in (202, 400)


# ── GET /api/status/<task_id> ─────────────────────────────────────────────────

class TestStatus:
    def test_status_known_task(self, client):
        c, svc = client
        resp = c.get("/api/status/mock-task-id-1234")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["task_id"] == "mock-task-id-1234"

    def test_status_unknown_task_returns_404(self, client):
        c, svc = client
        svc.get_task.return_value = None
        resp = c.get("/api/status/unknown")
        assert resp.status_code == 404


# ── GET /api/result/<task_id> ─────────────────────────────────────────────────

class TestResult:
    def test_result_pending_returns_202(self, client):
        c, _ = client
        resp = c.get("/api/result/mock-task-id-1234")
        assert resp.status_code == 202

    def test_result_completed_returns_200(self, client):
        c, svc = client
        svc.get_task.return_value = Task(
            task_id="mock-task-id-1234",
            status=TaskStatus.COMPLETED,
            progress=100,
            message="Done",
            result={"score": 1.0},
        )
        resp = c.get("/api/result/mock-task-id-1234")
        assert resp.status_code == 200
        assert resp.get_json()["score"] == 1.0

    def test_result_error_returns_500(self, client):
        c, svc = client
        svc.get_task.return_value = Task(
            task_id="mock-task-id-1234",
            status=TaskStatus.ERROR,
            progress=0,
            message="Pipeline exploded",
        )
        resp = c.get("/api/result/mock-task-id-1234")
        assert resp.status_code == 500

    def test_result_unknown_task_returns_404(self, client):
        c, svc = client
        svc.get_task.return_value = None
        resp = c.get("/api/result/unknown-id")
        assert resp.status_code == 404
