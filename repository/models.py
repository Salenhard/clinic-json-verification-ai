"""Domain model for a verification Task.

This is a plain dataclass — no framework coupling, no DB logic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class TaskStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    ABORTED = "aborted"
    ERROR = "error"


@dataclass
class Task:
    task_id: str
    status: TaskStatus = TaskStatus.PENDING
    progress: int = 0
    message: str = "Created"
    result: Any = None
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime | None = None
    json_path: str

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "status": self.status.value,
            "progress": self.progress,
            "message": self.message,
            "result": self.result,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "json_path": self.json_path,
        }
