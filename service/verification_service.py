"""VerificationService — orchestrates the AI pipeline and updates the repository."""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from typing import Any

import json
import os
from config import Settings
from pipeline import LLMAdapter, LLMAdapterFactory, GeminiAdapter, OpenAICompatibleAdapter, ClaudeAdapter
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
    llm_provider: str | None = None
    api_key: str | None = None
    chunk_size: int | None = None
    overlap: int | None = None
    requests_per_minute: int | None = None
    target_score: int | None = None
    max_iterations: int | None = None

class VerificationService:

    def __init__(
        self,
        repository: AbstractTaskRepository,
        settings: Settings,
        default_adapter: LLMAdapter | None = None,
        genai_client=None,
    ) -> None:
        self._repo = repository
        self._settings = settings
        configure_limiter(settings.requests_per_minute)

        if default_adapter is not None:
            self._default_adapter = default_adapter
        elif genai_client is not None:
            self._default_adapter = GeminiAdapter(
                client=genai_client,
                model=settings.gemini_model,
            )
            logger.warning(
                "genai_client передан напрямую — устаревший способ. "
                "Передавай default_adapter=GeminiAdapter(...) явно."
            )
        else:
            raise ValueError("Нужен default_adapter или genai_client.")

    def submit(self, req: VerificationRequest) -> str:
        """Create a task, start background processing, return task_id."""
        task_id = str(uuid.uuid4())
        self._repo.create(Task(task_id=task_id, message="Создано"))

        adapter = self._resolve_adapter(req)

        thread = threading.Thread(
            target=self._run_pipeline,
            args=(task_id, req, adapter),
            daemon=True,
            name=f"task-{task_id[:8]}",
        )
        thread.start()
        logger.info("Task %s submitted (model=%s)", task_id, adapter.model_name)
        return task_id

    def get_task(self, task_id: str) -> Task | None:
        return self._repo.get(task_id)
    
    def get_all_tasks(self, page, page_size) -> list[Task]:
        return self._repo.get_all(page=page, page_size=page_size)

    def delete_task(self, task_id: str) -> None:
        try:
            self._repo.delete(task_id)
        except Exception as e:
            raise e

    def cancel_task(self, task_id: str) -> None:
        task = self._repo.get(task_id)

        if task is None:
            raise Exception("Task not found")

        if task.status not in (TaskStatus.PENDING, TaskStatus.PROCESSING):
            raise Exception("Нельзя отменить задачу в этом статусе")

        self._update(
            task_id,
            status=TaskStatus.ABORTED,
            progress=task.progress,
            message="Задача отменена пользователем",
    )

    def _resolve_adapter(self, req: VerificationRequest) -> LLMAdapter:
        if req.model is not None:
            return LLMAdapterFactory.create(
                req.llm_provider,
                model=req.model,
                api_key=req.api_key or self._settings.gemini_api_key,
            )

        return self._default_adapter

    def _run_pipeline(
        self, task_id: str, req: VerificationRequest, adapter: LLMAdapter
    ) -> None:
        self._update(task_id, TaskStatus.PROCESSING, 5, "Запуск пайплайна")

        context: dict = {
            "input_data": req.input_data,
            "recommendations": req.recommendations,
            "recommendations_bytes": req.recommendations_bytes,
            "recommendations_filename": req.recommendations_filename,
            "chunk_size": req.chunk_size,
            "overlap": req.overlap,
            "max_iterations": req.max_iterations,
            "target_score": req.target_score,
            "requests_per_minute": req.requests_per_minute
        }

        stages = self._build_stages(adapter, context)

        try:
            self._check_aborted(task_id)
            context = stages[0].run(context)
            self._check_aborted(task_id)
            context = self._refinement_loop(task_id, context, stages)
            self._check_aborted(task_id)
            context = stages[4].run(context)

            folder = "results"
            final_result = context.get("final_result")
            self._save_result(folder, task_id, final_result)

            self._update(
                task_id, TaskStatus.COMPLETED, 100,
                "Верификация завершена",
                result=final_result,
                json_path=os.path.join(folder, f"{task_id}.json"),
            )

        except Exception as exc:
            task = self._repo.get(task_id)

            if task and task.status == TaskStatus.ABORTED:
                logger.info("Task %s aborted", task_id)
                return
                
            logger.exception("Pipeline failed for task %s", task_id)
            self._update(task_id, TaskStatus.ERROR, 0, f"Ошибка: {exc}")

    def _save_result(self, folder_name: str, task_id: str, result: Any) -> None:
        os.makedirs(folder_name, exist_ok=True)

        filename = os.path.join(folder_name, f"{task_id}.json")

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=4)

    def _check_aborted(self, task_id: str):
        task = self._repo.get(task_id)
        if task and task.status == TaskStatus.ABORTED:
            raise Exception("Task aborted")

    def _refinement_loop(self, task_id: str, context: dict, stages: list) -> dict:
        """Run analysis → validation → correction up to max_iterations times."""
        max_iter = context["max_iterations"]
        target = context["target_score"]
        folder = "result"
        for i in range(max_iter):
            self._check_aborted(task_id)
            progress = int(20 + (i / max(max_iter - 1, 1)) * 60)
            self._update(
                task_id, TaskStatus.PROCESSING, progress,
                f"Итерация {i + 1}: анализ и исправление",
            )

            context = stages[1].run(context)   # AnalysisStage
            self._save_result(folder, f"{task_id} AnalysisStage iter: {i}", context.get("analysis"))
            context = stages[2].run(context)   # JsonValidator
            context = stages[3].run(context)   # CorrectionStage
            self._save_result(folder, f"{task_id} CorrectionStage iter: {i}", context.get("corrected_data"))
            if "corrected_data" in context:
                context["input_data"] = context["corrected_data"]
                context["original_data"] = context["corrected_data"]

            score = context.get("analysis", {}).get("completeness_score", 0.0)
            logger.info("Iteration %d/%d — score: %.2f", i + 1, max_iter, score)

            if score >= target:
                self._update(task_id, TaskStatus.PROCESSING, 80, "Достигнута достаточная полнота")
                break

        return context

    def _build_stages(self, adapter: LLMAdapter, context: dict) -> list:
        """Instantiate a fresh set of pipeline stages for each job."""
        # Стейджи теперь получают адаптер вместо (client, model)
        kwargs = {
            "adapter": adapter,
            "requests_per_minute": context["requests_per_minute"],
        }
        return [
            JsonPreprocessor(**kwargs),
            AnalysisStage(**kwargs),
            JsonValidator(**kwargs),
            CorrectionStage(**kwargs),
            FinalizationStage(**kwargs),
        ]

    def _update(
        self,
        task_id: str,
        status: TaskStatus,
        progress: int,
        message: str,
        result: Any = None,
        json_path: str | None = None
    ) -> None:
        self._repo.update_status(
            task_id,
            status=status,
            progress=progress,
            message=message,
            result=result,
            json_path=json_path,
        )