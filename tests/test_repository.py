"""Tests for SQLiteTaskRepository.

Run with:  pytest tests/test_repository.py -v
"""

from __future__ import annotations

import tempfile
import os
import pytest

from repository import SQLiteTaskRepository, Task, TaskStatus


@pytest.fixture
def repo():
    """Temporary on-disk SQLite DB — deleted after each test."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    r = SQLiteTaskRepository(db_path=db_path)
    yield r
    os.unlink(db_path)


def _make_task(task_id: str = "abc-123") -> Task:
    return Task(task_id=task_id, message="Test task")


class TestCreate:
    def test_create_then_get(self, repo):
        task = _make_task()
        repo.create(task)
        fetched = repo.get(task.task_id)
        assert fetched is not None
        assert fetched.task_id == task.task_id
        assert fetched.status == TaskStatus.PENDING
        assert fetched.progress == 0

    def test_get_missing_returns_none(self, repo):
        assert repo.get("nonexistent") is None

    def test_duplicate_create_raises(self, repo):
        task = _make_task()
        repo.create(task)
        with pytest.raises(Exception):
            repo.create(task)


class TestUpdateStatus:
    def test_update_to_processing(self, repo):
        task = _make_task()
        repo.create(task)
        repo.update_status(task.task_id, status=TaskStatus.PROCESSING, progress=50, message="Half done")
        fetched = repo.get(task.task_id)
        assert fetched.status == TaskStatus.PROCESSING
        assert fetched.progress == 50
        assert fetched.message == "Half done"
        assert fetched.updated_at is not None

    def test_update_with_result(self, repo):
        task = _make_task()
        repo.create(task)
        result_payload = {"score": 0.95, "issues": []}
        repo.update_status(
            task.task_id,
            status=TaskStatus.COMPLETED,
            progress=100,
            message="Done",
            result=result_payload,
        )
        fetched = repo.get(task.task_id)
        assert fetched.status == TaskStatus.COMPLETED
        assert fetched.result == result_payload

    def test_update_to_error(self, repo):
        task = _make_task()
        repo.create(task)
        repo.update_status(task.task_id, status=TaskStatus.ERROR, progress=0, message="Boom")
        fetched = repo.get(task.task_id)
        assert fetched.status == TaskStatus.ERROR

    def test_result_none_stays_none(self, repo):
        task = _make_task()
        repo.create(task)
        repo.update_status(task.task_id, status=TaskStatus.PROCESSING, progress=10, message="Running")
        fetched = repo.get(task.task_id)
        assert fetched.result is None


class TestTaskToDict:
    def test_to_dict_keys(self, repo):
        task = _make_task()
        repo.create(task)
        d = repo.get(task.task_id).to_dict()
        assert set(d.keys()) == {"task_id", "status", "progress", "message", "result", "created_at", "updated_at"}

    def test_to_dict_status_is_string(self, repo):
        task = _make_task()
        repo.create(task)
        d = repo.get(task.task_id).to_dict()
        assert isinstance(d["status"], str)
        assert d["status"] == "pending"
