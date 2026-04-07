"""Tests for VerificationService.

The pipeline stages and genai client are mocked — this is a pure unit test.

Run with:  pytest tests/test_service.py -v
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch, call

import pytest

from config import Settings
from repository import Task, TaskStatus
from service import VerificationService, VerificationRequest


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_repo():
    repo = MagicMock()
    repo.get.return_value = Task(task_id="t1", message="ok")
    return repo


@pytest.fixture
def mock_settings():
    return Settings(
        gemini_api_key="fake-key",
        gemini_model="test-model",
        requests_per_minute=10,
        db_path=":memory:",
        max_iterations=2,
        target_score=1.0,
    )


@pytest.fixture
def mock_client():
    return MagicMock()


def _make_stage(context_update: dict | None = None, score: float = 1.0):
    """Return a mock stage whose .run() merges context_update into the context."""
    stage = MagicMock()

    def run(ctx):
        updated = {**ctx, **(context_update or {})}
        updated.setdefault("analysis", {})["completeness_score"] = score
        return updated

    stage.run.side_effect = run
    return stage


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestSubmit:
    def test_submit_returns_task_id(self, mock_repo, mock_client, mock_settings):
        svc = VerificationService(mock_repo, mock_client, mock_settings)
        req = VerificationRequest(input_data={"a": 1}, recommendations="rec")

        with patch.object(svc, "_build_stages", return_value=[_make_stage() for _ in range(5)]):
            task_id = svc.submit(req)

        assert isinstance(task_id, str)
        assert len(task_id) == 36  # UUID4 format
        mock_repo.create.assert_called_once()

    def test_submit_creates_task_with_correct_id(self, mock_repo, mock_client, mock_settings):
        svc = VerificationService(mock_repo, mock_client, mock_settings)
        req = VerificationRequest(input_data={}, recommendations="rec")

        with patch.object(svc, "_build_stages", return_value=[_make_stage() for _ in range(5)]):
            task_id = svc.submit(req)

        created_task: Task = mock_repo.create.call_args[0][0]
        assert created_task.task_id == task_id


class TestPipeline:
    def _run_sync(self, svc, req, stages):
        """Run pipeline synchronously (without threading) for predictable test behaviour."""
        task_id = str("test-task-id")
        svc._repo.create(Task(task_id=task_id))
        svc._run_pipeline(task_id, req, "test-model")
        return task_id

    def test_pipeline_completes_on_high_score(self, mock_repo, mock_client, mock_settings):
        svc = VerificationService(mock_repo, mock_client, mock_settings)
        req = VerificationRequest(input_data={"x": 1}, recommendations="rec")

        stages = [_make_stage(score=1.0) for _ in range(5)]
        with patch.object(svc, "_build_stages", return_value=stages):
            self._run_sync(svc, req, stages)

        # Last update_status call should be COMPLETED
        last_call = mock_repo.update_status.call_args
        assert last_call.kwargs["status"] == TaskStatus.COMPLETED
        assert last_call.kwargs["progress"] == 100

    def test_pipeline_marks_error_on_exception(self, mock_repo, mock_client, mock_settings):
        svc = VerificationService(mock_repo, mock_client, mock_settings)
        req = VerificationRequest(input_data={}, recommendations="rec")

        broken_stage = MagicMock()
        broken_stage.run.side_effect = RuntimeError("stage exploded")
        stages = [broken_stage] + [_make_stage() for _ in range(4)]

        with patch.object(svc, "_build_stages", return_value=stages):
            self._run_sync(svc, req, stages)

        last_call = mock_repo.update_status.call_args
        assert last_call.kwargs["status"] == TaskStatus.ERROR
        assert "stage exploded" in last_call.kwargs["message"]

    def test_refinement_loop_stops_early_on_target_score(self, mock_repo, mock_client, mock_settings):
        """With max_iterations=2 and score>=1.0 on first pass, loop should run only once."""
        svc = VerificationService(mock_repo, mock_client, mock_settings)
        req = VerificationRequest(input_data={}, recommendations="rec")

        call_counts = {"analysis": 0}

        def counting_run(ctx):
            call_counts["analysis"] += 1
            ctx["analysis"] = {"completeness_score": 1.0}
            return ctx

        analysis_stage = MagicMock()
        analysis_stage.run.side_effect = counting_run

        stages = [_make_stage(), analysis_stage, _make_stage(), _make_stage(), _make_stage()]

        with patch.object(svc, "_build_stages", return_value=stages):
            self._run_sync(svc, req, stages)

        assert call_counts["analysis"] == 1  # early exit after first iteration


class TestGetTask:
    def test_delegates_to_repo(self, mock_repo, mock_client, mock_settings):
        svc = VerificationService(mock_repo, mock_client, mock_settings)
        result = svc.get_task("t1")
        mock_repo.get.assert_called_once_with("t1")
        assert result == mock_repo.get.return_value
