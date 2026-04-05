"""Stage 5 — Assemble the final response payload.

FIX: the original mutated the client's corrected_json by injecting
     service fields (validated, validated_at, ...) directly into it.
     This changes the document structure which violates the core requirement.

     Now we build a separate wrapper object so the corrected document
     is returned intact under the 'document' key.
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

        # FIX: wrap result — never mutate the document itself
        context["final_result"] = {
            # The corrected document with original structure preserved
            "document": context["corrected_data"],

            # Validation report
            "validation": {
                "completeness_score": analysis.get("completeness_score", 0.0),
                "issues": context.get("validation_issues", []),
                "overall_comment": analysis.get("overall_comment", ""),
            },

            # What was changed and why
            "changelog": context.get("changelog", []),

            # Service metadata (NOT injected into the document)
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
