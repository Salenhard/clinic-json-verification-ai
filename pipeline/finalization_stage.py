from .base import BasePipelineStage
from datetime import datetime
from typing import Dict, Any

class FinalizationStage(BasePipelineStage):
    stage_name = "stage5_finalization"

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
    
        result = context["corrected_data"]
        result["validated"] = True
        result["validated_at"] = datetime.now().isoformat()
        result["compliance_note"] = "JSON проверен и дополнен в соответствии с клиническими рекомендациями (Gemini)"
        result["validation_issues"] = context.get("validation_issues", [])

        context["final_result"] = result
        return context