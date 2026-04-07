"""Abstract repository — defines the contract, hides the storage engine."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from .models import Task, TaskStatus


class AbstractTaskRepository(ABC):

    @abstractmethod
    def create(self, task: Task) -> None:
        """Persist a newly created Task."""

    @abstractmethod
    def get(self, task_id: str) -> Task | None:
        """Return a Task by its ID, or None if not found."""

    @abstractmethod
    def update_status(
        self,
        task_id: str,
        *,
        status: TaskStatus,
        progress: int,
        message: str,
        result: Any = None,
    ) -> None:
        """Update mutable fields of an existing Task."""
