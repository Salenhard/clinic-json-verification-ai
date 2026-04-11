import logging
from datetime import datetime, timezone
from typing import Dict, Any

from .base import BasePipelineStage

logger = logging.getLogger(__name__)


class FinalizationStage(BasePipelineStage):
    stage_name = "stage5_finalization"

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        changelog = context.get("changelog", [])
        supplement = context.get("analysis", {}).get("supplement_json", {})

        context["final_result"] = {
            "document": context["corrected_data"],

            "supplement": {
                "updates_applied": len(supplement.get("updates", [])),
                "additions_applied": len(supplement.get("additions", [])),
            },

            "changelog": changelog,

            "meta": {
                "validated_at": datetime.now(timezone.utc).isoformat(),
                "note": "JSON дополнен согласно клиническим рекомендациям.",
            },
        }

        logger.info(
            "Finalization: changes=%d  updates=%d  additions=%d",
            len(changelog),
            len(supplement.get("updates", [])),
            len(supplement.get("additions", [])),
        )
        return context
