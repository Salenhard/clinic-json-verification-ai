import logging
from collections import Counter
from datetime import datetime, timezone
from typing import Dict, Any

from .base import BasePipelineStage

logger = logging.getLogger(__name__)


class FinalizationStage(BasePipelineStage):
    stage_name = "stage5_finalization"

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        changelog = context.get("changelog", [])

        # Считаем реальные изменения из changelog, а не из патча
        action_counts = Counter(e.get("action") for e in changelog)
        fields_added = action_counts.get("added", 0)
        fields_modified = action_counts.get("modified", 0)
        records_added = action_counts.get("added_record", 0)

        # Сколько итераций прошло
        iterations_done = context.get("_iteration", 1)

        context["final_result"] = {
            "document": context["corrected_data"],

            "supplement": {
                "fields_added": fields_added,
                "fields_modified": fields_modified,
                "records_added": records_added,
                "total_changes": len(changelog),
                "iterations": iterations_done,
            },

            "changelog": changelog,

            "meta": {
                "validated_at": datetime.now(timezone.utc).isoformat(),
                "note": "JSON дополнен согласно клиническим рекомендациям.",
            },
        }

        logger.info(
            "Finalization: fields_added=%d  fields_modified=%d  records_added=%d  iterations=%d",
            fields_added, fields_modified, records_added, iterations_done,
        )
        return context
