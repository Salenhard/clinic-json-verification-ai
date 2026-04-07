"""Stage 4 — Correct and supplement the JSON using LLM.

  - Works with object_issues structure from AnalysisStage/JsonValidator.
  - Extracts only the objects that have issues (not the full 159-item list),
    avoiding truncation and keeping prompts focused.
  - Handles both list and dict documents (list bug from v1 is fixed).
  - After LLM correction, merges corrected objects back into the full list
    by object_index, preserving all unchanged objects.
  - Guard: if LLM changes the number of returned objects, falls back to original.
"""
import json
import logging
from copy import deepcopy
from typing import Dict, Any

from .base import BasePipelineStage

logger = logging.getLogger(__name__)

_SCHEMA = """\
Каждый объект описывает один клинический метод со следующими полями:
  method, method_type, diagnosis, patient_group, goal, conditions,
  contraindications, timing, dosage, recommendation_type,
  evidence_level (только латинские A|B|C|D), evidence_grade (1-5),
  source_quote, source_section, source_number, source_page, source_filename.\
"""

_PROMPT = """\
Ты — редактор клинических данных. Исправь и дополни JSON-объекты \
строго на основе клинических рекомендаций.

=== СХЕМА ===
{schema}

=== КЛИНИЧЕСКИЕ РЕКОМЕНДАЦИИ ===
{recommendations}

=== ОБЪЕКТЫ С ПРОБЛЕМАМИ ===
{objects_json}

=== НАЙДЕННЫЕ ПРОБЛЕМЫ ===
{issues_text}

=== ПРАВИЛА ===
1. Верни ровно столько объектов, сколько получил — тот же порядок.
2. НЕ удаляй и НЕ переименовывай поля.
3. НЕ изменяй поля без проблем — только те, что указаны в списке проблем.
4. Заполняй null/пустые значения ТОЛЬКО данными из рекомендаций выше.
5. evidence_level — исправляй кириллицу на латиницу (А→A, В→B, С→C).
6. Если поле исправить невозможно (нет данных в рекомендациях) — оставь как есть.

Верни ТОЛЬКО валидный JSON без markdown:
{{
  "corrected_objects": [ ... ],
  "changelog": [
    {{
      "method": "название метода",
      "field": "имя поля",
      "old_value": "...",
      "new_value": "...",
      "reason": "обоснование из рекомендаций"
    }}
  ]
}}
"""


class CorrectionStage(BasePipelineStage):
    stage_name = "stage4_correction"
    MAX_OUTPUT_TOKENS = 32768

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        original = context["original_data"]
        analysis = context["analysis"]
        object_issues: list[dict] = analysis.get("object_issues", [])

        # Filter to entries that actually have critical or warning issues
        actionable = [
            entry for entry in object_issues
            if any(
                i.get("severity") in ("critical", "warning")
                for i in entry.get("issues", [])
            )
        ]

        if not actionable:
            logger.info("CorrectionStage: no actionable issues — skipping LLM call")
            context["corrected_data"] = deepcopy(original)
            context["changelog"] = []
            return context

        # Build the subset of objects to send to LLM
        is_list = isinstance(original, list)
        objects_to_fix: list[dict] = []
        index_map: list[int] = []   # position in objects_to_fix → index in original

        for entry in actionable:
            idx = entry.get("object_index")
            if is_list and idx is not None and 0 <= idx < len(original):
                objects_to_fix.append(original[idx])
                index_map.append(idx)
            elif isinstance(original, dict):
                # dict document: treat the whole doc as one object
                objects_to_fix = [original]
                index_map = []
                break

        if not objects_to_fix:
            logger.warning("CorrectionStage: no valid object_index found — using original")
            context["corrected_data"] = deepcopy(original)
            context["changelog"] = []
            return context

        # Format issues for prompt
        issues_lines: list[str] = []
        for entry in actionable:
            method = entry.get("method", "?")
            for iss in entry.get("issues", []):
                sev = iss.get("severity", "info").upper()
                field = iss.get("field") or "—"
                desc = iss.get("description", "")
                sugg = iss.get("suggestion", "")
                issues_lines.append(f"[{sev}] {method} / {field}: {desc}")
                if sugg:
                    issues_lines.append(f"  → {sugg}")

        objects_json = json.dumps(objects_to_fix, ensure_ascii=False, indent=2)

        rec_text = context.get("recommendations_full_text", "")
        # Собираем релевантные чанки по source_section объектов с проблемами
        if context.get("recommendation_chunks") and len(rec_text) > 15_000:
            relevant_sections = set()
            for obj in objects_to_fix:
                s = obj.get("source_section") or ""
                if s:
                    relevant_sections.add(s.lower().strip())

            if relevant_sections:
                chunks = context["recommendation_chunks"]
                relevant_texts = []
                for chunk in chunks:
                    chunk_lower = chunk.text.lower()
                    if any(sec in chunk_lower for sec in relevant_sections):
                        relevant_texts.append(chunk.text)
                if relevant_texts:
                    rec_text = "\n\n---\n\n".join(relevant_texts)

        if len(rec_text) > 20_000:
            rec_text = rec_text[:20_000]

        prompt = _PROMPT.format(
            schema=_SCHEMA,
            recommendations=rec_text,
            objects_json=objects_json,
            issues_text="\n".join(issues_lines),
        )

        result = self._execute_with_retry(prompt)

        corrected_objects = result.get("corrected_objects")

        # Validate LLM response type and length
        if not isinstance(corrected_objects, list):
            logger.warning(
                "CorrectionStage: LLM returned invalid corrected_objects (type %s) — using original",
                type(corrected_objects).__name__,
            )
            context["corrected_data"] = deepcopy(original)
            context["changelog"] = []
            return context

        if len(corrected_objects) != len(objects_to_fix):
            logger.warning(
                "CorrectionStage: LLM changed object count (%d → %d) — using original",
                len(objects_to_fix), len(corrected_objects),
            )
            context["corrected_data"] = deepcopy(original)
            context["changelog"] = []
            return context

        # Merge corrected objects back into the full document
        if is_list:
            corrected_full = deepcopy(original)
            # Для восстановления дропнутых полей берём pristine_original
            pristine = context.get("pristine_original", original)
            for pos, original_idx in enumerate(index_map):
                corrected_obj = corrected_objects[pos]
                # Guard: ensure no fields were dropped by LLM
                pristine_obj = pristine[original_idx] if isinstance(pristine, list) and original_idx < len(pristine) else original[original_idx]
                for key in pristine_obj:
                    if key not in corrected_obj:
                        logger.warning(
                            "CorrectionStage: LLM dropped field '%s' in '%s' — restoring",
                            key, pristine_obj.get("method", f"#{original_idx}"),
                        )
                        corrected_obj[key] = pristine_obj[key]
                corrected_full[original_idx] = corrected_obj
        else:
            # dict document: single-object case
            corrected_full = corrected_objects[0]
            for key in original:
                if key not in corrected_full:
                    logger.warning("CorrectionStage: LLM dropped key '%s' — restoring", key)
                    corrected_full[key] = original[key]

        context["corrected_data"] = corrected_full
        context["changelog"] = result.get("changelog", [])

        logger.info(
            "CorrectionStage: corrected %d objects, %d changelog entries",
            len(objects_to_fix), len(context["changelog"]),
        )
        return context