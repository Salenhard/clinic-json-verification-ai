"""SQLite-backed TaskRepository.

Thread-safe via WAL mode + per-call connection lifecycle.
Swap this for a Postgres implementation by subclassing AbstractTaskRepository.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Any

from .base import AbstractTaskRepository
from .models import Task, TaskStatus

logger = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id    TEXT PRIMARY KEY,
    status     TEXT NOT NULL DEFAULT 'pending',
    progress   INTEGER NOT NULL DEFAULT 0,
    message    TEXT,
    result     TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT,
    json_path TEXT
);
"""


class SQLiteTaskRepository(AbstractTaskRepository):
    """Concrete repository backed by a local SQLite file."""
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._init_schema()


    @contextmanager
    def _connection(self):
        conn = sqlite3.connect(self._db_path, timeout=10)
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

    def _init_schema(self) -> None:
        with self._connection() as conn:
            conn.executescript(_DDL)
        logger.info("SQLite schema initialised at %s", self._db_path)

    def create(self, task: Task) -> None:
        with self._connection() as conn:
            conn.execute(
                """INSERT INTO tasks
                   (task_id, status, progress, message, created_at, json_path)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    task.task_id,
                    task.status.value,
                    task.progress,
                    task.message,
                    task.created_at.isoformat(),
                    task.json_path
                ),
            )
        logger.debug("Task created: %s", task.task_id)

    def delete(self, task_id: str) -> None:
        task = get_task(task_id)
        if task.get_status() in (PENDING, PROCESSING):
            raise Exception("Нельзя удалить задачу в процессе обработки")
        with self._connection() as conn:
            conn.execute(
                "DELETE from tasks where task_id = ?",
                (task_id,)
            )
            conn.commit()

    def get_all(self, page: int = 1, page_size: int = 10) -> dict:
        offset = (page - 1) * page_size

        with self._connection() as conn:
            rows = conn.execute(
                """SELECT task_id, status, progress, message, result,
                          created_at, updated_at, json_path
                   FROM tasks
                   ORDER BY created_at DESC
                   LIMIT ? OFFSET ?""",
                (page_size, offset),
            ).fetchall()
            total = conn.execute(
                "SELECT COUNT(*) FROM tasks"
            ).fetchone()[0]

        tasks = [
            Task(
                task_id=row["task_id"],
                status=TaskStatus(row["status"]),
                progress=row["progress"],
                message=row["message"] or "",
                result=json.loads(row["result"]) if row["result"] else None,
                created_at=datetime.fromisoformat(row["created_at"]),
                updated_at=datetime.fromisoformat(row["updated_at"]) if row["updated_at"] else None,
                json_path=row["json_path"],
            )
            for row in rows
        ]

        return {
            "items": tasks,
            "page": page,
            "page_size": page_size,
            "total": total,
            "pages": (total + page_size - 1) // page_size,
        }

        tasks = []
        for row in rows:
            tasks.append(
                Task(
                    task_id=row["task_id"],
                    status=TaskStatus(row["status"]),
                    progress=row["progress"],
                    message=row["message"] or "",
                    result=json.loads(row["result"]) if row["result"] else None,
                    created_at=datetime.fromisoformat(row["created_at"]),
                    updated_at=datetime.fromisoformat(row["updated_at"]) if row["updated_at"] else None,
                    json_path=row["json_path"],
                )
            )

        return tasks

    def get(self, task_id: str) -> Task | None:
        with self._connection() as conn:
            row = conn.execute(
                """SELECT task_id, status, progress, message, result,
                          created_at, updated_at, json_path
                   FROM tasks WHERE task_id = ?""",
                (task_id,),
            ).fetchone()

        if row is None:
            return None

        return Task(
            task_id=row["task_id"],
            status=TaskStatus(row["status"]),
            progress=row["progress"],
            message=row["message"] or "",
            result=json.loads(row["result"]) if row["result"] else None,
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]) if row["updated_at"] else None,
            json_path=row["json_path"],
        )

    def update_status(
        self,
        task_id: str,
        *,
        status: TaskStatus,
        progress: int,
        message: str,
        result: Any = None,
        json_path: str,
    ) -> None:
        result_str = json.dumps(result, ensure_ascii=False) if result is not None else None
        with self._connection() as conn:
            conn.execute(
                """UPDATE tasks
                   SET status=?, progress=?, message=?, result=?, updated_at=?, json_path=?
                   WHERE task_id=?""",
                (
                    status.value,
                    progress,
                    message,
                    result_str,
                    datetime.now().isoformat(),
                    json_path,
                    task_id,
                ),
            )
        logger.debug("Task %s → %s (%d%%)", task_id, status.value, progress)
