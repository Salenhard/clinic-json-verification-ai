"""Stage 5 — Assemble the final result.

Changes vs v1:
  - Includes object_issues and missing_methods in the validation block.
  - Flattens object_issues into a summary (counts by severity) for quick overview.
  - Keeps the full object_issues list for detailed inspection.
"""
import logging
from datetime import datetime, timezone
from typing import Dict, Any

from .base import BasePipelineStage

logger = logging.getLogger(__name__)


class FinalizationStage(BasePipelineStage):
    stage_name = "stage5_finalization"

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        analysis = context.get("analysis", {})
        object_issues: list[dict] = analysis.get("object_issues", [])

        # Flatten counts for summary
        n_critical = n_warning = n_info = 0
        for entry in object_issues:
            for iss in entry.get("issues", []):
                sev = iss.get("severity", "info")
                if sev == "critical":
                    n_critical += 1
                elif sev == "warning":
                    n_warning += 1
                else:
                    n_info += 1

        context["final_result"] = {
            "document": context["corrected_data"],

            "validation": {
                "completeness_score": analysis.get("completeness_score", 0.0),
                "summary": {
                    "objects_with_issues": len(object_issues),
                    "critical": n_critical,
                    "warning": n_warning,
                    "info": n_info,
                },
                "object_issues": object_issues,
                "missing_methods": analysis.get("missing_methods", []),
                "overall_comment": analysis.get("overall_comment", ""),
            },

            "changelog": context.get("changelog", []),

            "meta": {
                "validated_at": datetime.now(timezone.utc).isoformat(),
                "model": getattr(self.adapter, "model_name", "unknown"),
                "note": "JSON проверен и дополнен согласно клиническим рекомендациям.",
            },
        }

        logger.info(
            "Finalization: score=%.2f  critical=%d  warning=%d  changes=%d",
            analysis.get("completeness_score", 0.0),
            n_critical, n_warning,
            len(context.get("changelog", [])),
        )
        return context