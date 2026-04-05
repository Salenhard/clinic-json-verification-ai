from .base import BasePipelineStage
from datetime import datetime
from typing import Dict, Any

class FinalizationStage(BasePipelineStage):
    stage_name = "stage5_finalization"

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        self.update_progress(context, 95, "Финальная сборка результата...")

        result = context["corrected_data"]
        result["validated"] = True
        result["validated_at"] = datetime.now().isoformat()
        result["compliance_note"] = "JSON проверен и дополнен в соответствии с клиническими рекомендациями (Gemini)"
        result["validation_issues"] = context.get("validation_issues", [])

        context["final_result"] = result
        self.update_progress(context, 100, "Обработка завершена успешно")
        return context