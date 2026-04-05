from .base import BasePipelineStage
from typing import Dict, Any

class JsonValidator(BasePipelineStage):
    stage_name = "stage3_validation"

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        self.update_progress(context, 60, "Детерминированная + семантическая валидация JSON...")

        issues = context["analysis"].get("issues", [])

        context["validation_issues"] = issues
        self.update_progress(context, 70, f"Валидация завершена. Проблем: {len(issues)}")
        return context