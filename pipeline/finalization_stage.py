import logging
from datetime import datetime, timezone
from typing import Dict, Any

from .base import BasePipelineStage

logger = logging.getLogger(__name__)


class FinalizationStage(BasePipelineStage):
    stage_name = "stage5_finalization"

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        analysis = context.get("analysis", {})

        context["final_result"] = {
            "document": context["corrected_data"],

            "validation": {
                "completeness_score": analysis.get("completeness_score", 0.0),
                "issues": context.get("validation_issues", []),
                "overall_comment": analysis.get("overall_comment", ""),
            },

            "changelog": context.get("changelog", []),
            
            "meta": {
                "validated_at": datetime.now(timezone.utc).isoformat(),
                "note": "JSON проверен и дополнен согласно клиническим рекомендациям.",
            },
        }

        logger.info(
            "Finalization: score=%.2f  changes=%d",
            analysis.get("completeness_score", 0.0),
            len(context.get("changelog", [])),
        )
        return context
