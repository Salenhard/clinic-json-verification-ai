"""Stage 2 — Analyse JSON against clinical guidelines.

Key design for weak models:
  - Objects are pre-filtered per chunk by source_section/source_number
    so the LLM sees only relevant objects, not the full 100+ list.
  - The prompt is simplified: fewer instructions, clearer output format.
  - completeness_score is NOT computed by LLM — it's calculated
    deterministically in Python after all chunks are merged.
  - Issues are grouped per object: object_issues = [{method, object_index, issues}].
"""
import json
import logging
from typing import Dict, Any, List

from .base import BasePipelineStage

logger = logging.getLogger(__name__)

_SCHEMA = """\
Каждый объект описывает один клинический метод:
  method, method_type, diagnosis, patient_group, goal, conditions,
  contraindications, timing, dosage, recommendation_type,
  evidence_level (только латинские A|B|C|D),
  evidence_grade (1-5), source_quote, source_section, source_number.\
"""

_PROMPT_TEMPLATE = """\
Ты — валидатор клинических данных.

=== ФРАГМЕНТ РЕКОМЕНДАЦИЙ (часть {chunk_index} из {total_chunks}) ===
{chunk_text}

=== JSON-ОБЪЕКТЫ ДЛЯ ПРОВЕРКИ ===
{json_data}

=== ЗАДАЧА ===
Сравни каждый объект с текстом рекомендаций выше. Найди:
1. Ошибки в полях (неверные значения, пустые обязательные поля,
   кириллица в evidence_level, пропущенная дозировка для drug/treatment).
2. Методы из рекомендаций, которых нет ни в одном объекте.

Верни ТОЛЬКО валидный JSON (без markdown):
{{
  "object_issues": [
    {{
      "method": "название",
      "object_index": 0,
      "issues": [
        {{
          "field": "имя поля",
          "severity": "critical или warning или info",
          "description": "что не так",
          "suggestion": "что исправить"
        }}
      ]
    }}
  ],
  "missing_methods": ["метод из рекомендаций, отсутствующий в JSON"]
}}
"""


def _filter_objects_for_chunk(
    objects: list, chunk_text: str
) -> tuple:
    """Return (filtered_objects, original_indices) relevant to this chunk."""
    chunk_lower = chunk_text.lower()
    filtered = []
    indices = []

    for idx, obj in enumerate(objects):
        if not isinstance(obj, dict):
            continue
        section = (obj.get("source_section") or "").strip().lower()
        number = (obj.get("source_number") or "").strip()
        method = (obj.get("method") or "").strip().lower()

        matched = False
        if section and section in chunk_lower:
            matched = True
        elif number and number in chunk_text:
            matched = True
        elif method and len(method) > 5 and method in chunk_lower:
            matched = True

        if matched:
            filtered.append(obj)
            indices.append(idx)

    if not filtered:
        return objects, list(range(len(objects)))

    return filtered, indices


def _compute_deterministic_score(analysis: dict, total_objects: int) -> float:
    """Compute completeness score from issue counts — no LLM needed."""
    if total_objects == 0:
        return 1.0

    n_critical = 0
    n_warning = 0

    for entry in analysis.get("object_issues", []):
        for iss in entry.get("issues", []):
            sev = iss.get("severity", "info")
            if sev == "critical":
                n_critical += 1
            elif sev == "warning":
                n_warning += 1

    n_missing = len(analysis.get("missing_methods", []))
    penalty = n_critical * 0.1 + n_warning * 0.03 + n_missing * 0.05
    return round(max(0.0, 1.0 - penalty), 3)


class AnalysisStage(BasePipelineStage):
    stage_name = "stage2_analysis"
    MAX_OUTPUT_TOKENS = 32768

    def _build_prompt(
        self,
        chunk_text: str,
        chunk_index: int,
        total_chunks: int,
        json_data: str,
    ) -> str:
        return _PROMPT_TEMPLATE.format(
            schema=_SCHEMA,
            chunk_text=chunk_text,
            chunk_index=chunk_index + 1,
            total_chunks=total_chunks,
            json_data=json_data,
        )

    @staticmethod
    def _merge_results(results: list) -> dict:
        method_map = {}
        all_missing = set()

        for r in results:
            for obj in r.get("object_issues", []):
                method = obj.get("method", "")
                if not method:
                    continue
                if method not in method_map:
                    method_map[method] = {
                        "object_index": obj.get("object_index"),
                        "issues_by_key": {},
                    }
                for iss in obj.get("issues", []):
                    key = (iss.get("field", ""), iss.get("description", "")[:80])
                    method_map[method]["issues_by_key"][key] = iss

            all_missing.update(r.get("missing_methods", []))

        severity_order = {"critical": 0, "warning": 1, "info": 2}
        object_issues = []
        for method, data in method_map.items():
            issues = list(data["issues_by_key"].values())
            issues.sort(key=lambda x: severity_order.get(x.get("severity", "info"), 2))
            if issues:
                object_issues.append({
                    "method": method,
                    "object_index": data["object_index"],
                    "issues": issues,
                })

        return {
            "object_issues": object_issues,
            "missing_methods": sorted(all_missing),
        }

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        all_objects = context["original_data"]
        chunks = context["recommendation_chunks"]

        if not isinstance(all_objects, list):
            all_objects_list = [all_objects]
        else:
            all_objects_list = all_objects

        chunk_results = []
        for chunk in chunks:
            filtered, indices = _filter_objects_for_chunk(
                all_objects_list, chunk.text
            )

            annotated = []
            for obj, orig_idx in zip(filtered, indices):
                annotated.append({**obj, "_object_index": orig_idx})

            json_data = json.dumps(annotated, ensure_ascii=False, indent=2)

            prompt = self._build_prompt(
                chunk.text, chunk.index, len(chunks), json_data
            )

            logger.info(
                "%s: chunk %d/%d — %d objects (of %d total), %d chars",
                self.stage_name, chunk.index + 1, len(chunks),
                len(filtered), len(all_objects_list), chunk.char_count,
            )

            try:
                result = self._execute_with_retry(prompt)
                chunk_results.append(result)
            except Exception as e:
                logger.error("%s chunk %d failed: %s", self.stage_name, chunk.index, e)

        if not chunk_results:
            raise Exception(f"{self.stage_name}: all chunks failed")

        analysis = self._merge_results(chunk_results)

        analysis["completeness_score"] = _compute_deterministic_score(
            analysis, len(all_objects_list)
        )
        analysis["overall_comment"] = (
            f"Проанализировано {len(chunks)} фрагментов, "
            f"{len(all_objects_list)} объектов."
        )

        context["analysis"] = analysis
        logger.info(
            "Analysis: score=%.2f  objects_with_issues=%d  missing_methods=%d",
            analysis["completeness_score"],
            len(analysis["object_issues"]),
            len(analysis["missing_methods"]),
        )
        return context