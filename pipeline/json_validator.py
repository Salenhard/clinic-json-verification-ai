import logging
from typing import Dict, Any

from .base import BasePipelineStage

logger = logging.getLogger(__name__)


class JsonValidator(BasePipelineStage):
    stage_name = "stage3_validation"

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        analysis = context.get("analysis", {})
        original = context.get("original_data", {})

        issues = list(analysis.get("issues", []))

        if isinstance(original, dict):
            for field in analysis.get("missing_fields", []):
                if field in original and original[field] is not None:
                    issues.append({
                        "severity": "info",
                        "field": field,
                        "description": f"Поле '{field}' помечено как отсутствующее, но присутствует в документе.",
                        "suggestion": "Проверьте, соответствует ли значение рекомендациям.",
                    })

        severity_order = {"critical": 0, "warning": 1, "info": 2}
        issues.sort(key=lambda x: severity_order.get(x.get("severity", "info"), 2))

        context["validation_issues"] = issues
        logger.info(
            "Validator: %d issues (%d critical, %d warning)",
            len(issues),
            sum(1 for i in issues if i.get("severity") == "critical"),
            sum(1 for i in issues if i.get("severity") == "warning"),
        )
        return context
