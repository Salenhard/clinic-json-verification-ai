"""Stage 3 — Structural validation (pure Python, no LLM).

Enhancements vs v1:
  - Validates enum fields against allowed value sets.
  - Detects Cyrillic characters in evidence_level (А/В/С vs A/B/C).
  - Checks required fields for null / empty.
  - Injects findings as additional issues into analysis.object_issues,
    using the same structure that AnalysisStage produces.
  - Deduplicates against LLM-reported issues before injecting.
"""
import logging
from typing import Dict, Any

from .base import BasePipelineStage

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Allowed values for enum fields
# ---------------------------------------------------------------------------
_VALID_METHOD_TYPE = {
    "diagnostics", "drug", "procedure", "surgery",
    "treatment", "rehabilitation", "monitoring", "immobilization",
}
_VALID_RECOMMENDATION_TYPE = {
    "recommended", "conditional", "may_be_considered", "not_recommended",
}
_VALID_EVIDENCE_LEVEL_LATIN = {"A", "B", "C", "D"}
# Cyrillic lookalikes that appear due to encoding errors
_CYRILLIC_EVIDENCE = {"А", "В", "С", "D"}   # А В С — Cyrillic; D — Latin
_VALID_EVIDENCE_GRADE = {"1", "2", "3", "4", "5"}
_REQUIRED_FIELDS = {"method", "method_type", "diagnosis", "patient_group", "goal"}
_DRUG_FIELDS = {"drug", "treatment"}   # method_type values where dosage is critical


def _validate_object(obj: dict, idx: int) -> list[dict]:
    """Return a list of schema issues for a single triplet object."""
    issues = []

    def add(field: str, severity: str, description: str, suggestion: str) -> None:
        issues.append({
            "field": field,
            "severity": severity,
            "description": description,
            "suggestion": suggestion,
        })

    # 1. Required fields
    for field in _REQUIRED_FIELDS:
        val = obj.get(field)
        if val is None or val == "":
            add(field, "critical",
                f"Обязательное поле '{field}' пустое или отсутствует.",
                f"Заполнить поле '{field}'.")

    # 2. Enum: method_type
    mt = obj.get("method_type")
    if mt and mt not in _VALID_METHOD_TYPE:
        add("method_type", "warning",
            f"Недопустимое значение method_type: '{mt}'.",
            f"Допустимые значения: {', '.join(sorted(_VALID_METHOD_TYPE))}.")

    # 3. Enum: recommendation_type
    rt = obj.get("recommendation_type")
    if rt and rt not in _VALID_RECOMMENDATION_TYPE:
        add("recommendation_type", "warning",
            f"Недопустимое значение recommendation_type: '{rt}'.",
            f"Допустимые значения: {', '.join(sorted(_VALID_RECOMMENDATION_TYPE))}.")

    # 4. evidence_level — кириллица vs латиница
    el = obj.get("evidence_level")
    if el:
        if el in _CYRILLIC_EVIDENCE and el not in _VALID_EVIDENCE_LEVEL_LATIN:
            add("evidence_level", "critical",
                f"Кириллический символ в evidence_level: '{el}'. "
                "Кириллические А/В/С визуально похожи на латинские, но являются ошибкой.",
                f"Заменить '{el}' на латинский эквивалент: "
                f"'{el.translate(str.maketrans('АВС', 'ABC'))}'.")
        elif el not in _VALID_EVIDENCE_LEVEL_LATIN:
            add("evidence_level", "warning",
                f"Недопустимое значение evidence_level: '{el}'.",
                "Допустимые значения: A, B, C, D (латиница).")

    # 5. evidence_grade
    eg = obj.get("evidence_grade")
    if eg is not None and str(eg) not in _VALID_EVIDENCE_GRADE:
        add("evidence_grade", "warning",
            f"Недопустимое значение evidence_grade: '{eg}'.",
            "Допустимые значения: 1, 2, 3, 4, 5.")

    # 6. dosage критично для drug/treatment
    if mt in _DRUG_FIELDS and obj.get("dosage") is None:
        add("dosage", "warning",
            f"Поле dosage равно null для method_type='{mt}'.",
            "Заполнить дозировку/схему из клинических рекомендаций.")

    # 7. source_quote слишком короткая
    sq = obj.get("source_quote") or ""
    if len(sq.strip()) < 10:
        add("source_quote", "info",
            "Цитата source_quote пустая или слишком короткая.",
            "Добавить дословную цитату из источника.")

    return issues


class JsonValidator(BasePipelineStage):
    stage_name = "stage3_validation"

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        original = context.get("original_data", [])
        analysis = context.get("analysis", {})
        object_issues: list[dict] = analysis.get("object_issues", [])

        # Build lookup: method name -> entry in object_issues (for O(1) merging)
        issues_by_method: dict[str, dict] = {
            entry["method"]: entry for entry in object_issues
        }

        n_critical = n_warning = 0

        if isinstance(original, list):
            for idx, obj in enumerate(original):
                if not isinstance(obj, dict):
                    continue
                schema_issues = _validate_object(obj, idx)
                if not schema_issues:
                    continue

                method = obj.get("method", f"<object #{idx}>")

                if method not in issues_by_method:
                    issues_by_method[method] = {
                        "method": method,
                        "object_index": idx,
                        "issues": [],
                    }

                existing_keys: set[tuple] = {
                    (i.get("field", ""), i.get("description", "")[:80])
                    for i in issues_by_method[method]["issues"]
                }

                for iss in schema_issues:
                    key = (iss.get("field", ""), iss.get("description", "")[:80])
                    if key not in existing_keys:
                        issues_by_method[method]["issues"].append(iss)
                        existing_keys.add(key)

                for iss in schema_issues:
                    if iss["severity"] == "critical":
                        n_critical += 1
                    elif iss["severity"] == "warning":
                        n_warning += 1

        elif isinstance(original, dict):
            # Single-object document
            schema_issues = _validate_object(original, 0)
            if schema_issues:
                method = original.get("method", "<single object>")
                if method not in issues_by_method:
                    issues_by_method[method] = {
                        "method": method,
                        "object_index": 0,
                        "issues": [],
                    }
                for iss in schema_issues:
                    issues_by_method[method]["issues"].append(iss)
                    if iss["severity"] == "critical":
                        n_critical += 1
                    elif iss["severity"] == "warning":
                        n_warning += 1

        # Sort issues within each entry: critical first
        severity_order = {"critical": 0, "warning": 1, "info": 2}
        merged_object_issues = []
        for entry in issues_by_method.values():
            entry["issues"].sort(
                key=lambda x: severity_order.get(x.get("severity", "info"), 2)
            )
            merged_object_issues.append(entry)

        analysis["object_issues"] = merged_object_issues
        context["analysis"] = analysis

        total_issues = sum(len(e["issues"]) for e in merged_object_issues)
        logger.info(
            "Validator: schema checks added %d critical, %d warning; "
            "total object_issues entries=%d, total issues=%d",
            n_critical, n_warning,
            len(merged_object_issues), total_issues,
        )
        return context