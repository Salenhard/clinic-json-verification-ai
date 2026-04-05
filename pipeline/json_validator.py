from .base import BasePipelineStage
from typing import Dict, Any

class JsonValidator(BasePipelineStage):
    stage_name = "stage3_validation"

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:

        issues = context["analysis"].get("issues", [])

        context["validation_issues"] = issues
        return context