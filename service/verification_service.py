"""VerificationService — orchestrates the AI pipeline and updates the repository.

Design goals
------------
* Single Responsibility: this service knows about the pipeline and task states,
  it does NOT know about HTTP or SQLite internals.
* Injectable: receives repository and genai client via constructor → easy to mock in tests.
* Stateless per-task: each call to ``submit`` creates fresh stage instances.
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass
from typing import Any

from google import genai

from config import Settings
from pipeline import (
    JsonPreprocessor,
    AnalysisStage,
    JsonValidator,
    CorrectionStage,
    FinalizationStage,
    configure_limiter,
)
from repository import AbstractTaskRepository, Task, TaskStatus

logger = logging.getLogger(__name__)


@dataclass
class VerificationRequest:
    """Everything the service needs to start a verification job."""
    input_data: Any
    recommendations: str = ""
    recommendations_bytes: bytes | None = None
    recommendations_filename: str | None = None
    model: str | None = None


class VerificationService:
    """Coordinates the multi-stage AI verification pipeline."""

    def __init__(
        self,
        repository: AbstractTaskRepository,
        genai_client: genai.Client,
        settings: Settings,
    ) -> None:
        self._repo = repository
        self._client = genai_client
        self._settings = settings
        configure_limiter(settings.requests_per_minute)

    # ── Public API ────────────────────────────────────────────────────────────

    def submit(self, req: VerificationRequest) -> str:
        """Create a task, start background processing, return task_id."""
        task_id = str(uuid.uuid4())
        task = Task(task_id=task_id, message="Создано")
        self._repo.create(task)

        model = req.model or self._settings.gemini_model

        thread = threading.Thread(
            target=self._run_pipeline,
            args=(task_id, req, model),
            daemon=True,
            name=f"task-{task_id[:8]}",
        )
        thread.start()
        logger.info("Task %s submitted (model=%s)", task_id, model)
        return task_id

    def get_task(self, task_id: str) -> Task | None:
        return self._repo.get(task_id)

    # ── Pipeline execution (runs in a worker thread) ──────────────────────────

    def _run_pipeline(self, task_id: str, req: VerificationRequest, model: str) -> None:
        self._update(task_id, TaskStatus.PROCESSING, 5, "Запуск пайплайна")

        context: dict = {
            "input_data": req.input_data,
            "recommendations": req.recommendations,
            "recommendations_bytes": req.recommendations_bytes,
            "recommendations_filename": req.recommendations_filename,
        }

        stages = self._build_stages(model)

        try:
            # Stage 1 — preprocessing (runs once)
            context = stages[0].run(context)

            # Stages 2-4 — iterative refinement loop
            context = self._refinement_loop(task_id, context, stages)

            # Stage 5 — finalization (runs once)
            context = stages[4].run(context)

            self._update(
                task_id,
                TaskStatus.COMPLETED,
                100,
                "Верификация завершена",
                result=context.get("final_result"),
            )

        except Exception as exc:
            logger.exception("Pipeline failed for task %s", task_id)
            self._update(task_id, TaskStatus.ERROR, 0, f"Ошибка: {exc}")

    def _refinement_loop(self, task_id: str, context: dict, stages: list) -> dict:
        """Run analysis → validation → correction up to max_iterations times."""
        max_iter = self._settings.max_iterations
        target = self._settings.target_score

        for i in range(max_iter):
            progress = int(20 + (i / max(max_iter - 1, 1)) * 60)
            self._update(task_id, TaskStatus.PROCESSING, progress, f"Итерация {i + 1}: анализ и исправление")

            context = stages[1].run(context)   # AnalysisStage
            context = stages[2].run(context)   # JsonValidator
            context = stages[3].run(context)   # CorrectionStage

            # Promote corrected data for the next iteration
            if "corrected_data" in context:
                context["input_data"] = context["corrected_data"]
                context["original_data"] = context["corrected_data"]

            score = context.get("analysis", {}).get("completeness_score", 0.0)
            logger.info("Iteration %d/%d — completeness score: %.2f", i + 1, max_iter, score)

            if score >= target:
                self._update(task_id, TaskStatus.PROCESSING, 80, "Достигнута достаточная полнота")
                break

        return context

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _build_stages(self, model: str) -> list:
        """Instantiate a fresh set of pipeline stages for each job."""
        kwargs = {"model": model, "requests_per_minute": self._settings.requests_per_minute}
        return [
            JsonPreprocessor(self._client, **kwargs),
            AnalysisStage(self._client, **kwargs),
            JsonValidator(self._client, **kwargs),
            CorrectionStage(self._client, **kwargs),
            FinalizationStage(self._client, **kwargs),
        ]

    def _update(
        self,
        task_id: str,
        status: TaskStatus,
        progress: int,
        message: str,
        result: Any = None,
    ) -> None:
        self._repo.update_status(
            task_id,
            status=status,
            progress=progress,
            message=message,
            result=result,
        )
