"""Stage 4 — Correct and supplement the JSON using LLM.

v3 fixes after real-world test:
  - Deterministic Python fixes applied FIRST (Cyrillic evidence_level,
    enum normalization) — no LLM needed for these.
  - Remaining LLM-fixable issues sent in BATCHES of ≤15 objects
    so weak models don't choke on 159 objects at once.
  - After batched LLM calls, results merged back into full document.
"""
import json
import logging
from copy import deepcopy
from typing import Dict, Any

from .base import BasePipelineStage

logger = logging.getLogger(__name__)

# ── Deterministic fixes (no LLM needed) ─────────────────────────────────────

_CYRILLIC_TO_LATIN = str.maketrans("АВС", "ABC")


def _apply_deterministic_fixes(data: list | dict) -> tuple[list | dict, list[dict]]:
    """Fix issues that don't require LLM: Cyrillic evidence_level, etc.

    Returns (fixed_data, changelog).
    """
    changelog = []

    objects = data if isinstance(data, list) else [data]
    for obj in objects:
        if not isinstance(obj, dict):
            continue

        # Fix Cyrillic evidence_level
        el = obj.get("evidence_level")
        if el and any(c in el for c in "АВС"):
            new_el = el.translate(_CYRILLIC_TO_LATIN)
            changelog.append({
                "method": obj.get("method", "?"),
                "field": "evidence_level",
                "old_value": el,
                "new_value": new_el,
                "reason": "Кириллица заменена на латиницу (А→A, В→B, С→C)",
            })
            obj["evidence_level"] = new_el

    return data, changelog


# ── LLM correction ──────────────────────────────────────────────────────────

_SCHEMA = """\
Каждый объект описывает один клинический метод со следующими полями:
  method, method_type, diagnosis, patient_group, goal, conditions,
  contraindications, timing, dosage, recommendation_type,
  evidence_level (A|B|C|D), evidence_grade (1-5),
  source_quote, source_section, source_number.\
"""

_PROMPT = """\
Ты — редактор клинических данных. Исправь JSON-объекты \
строго на основе клинических рекомендаций.

=== КЛИНИЧЕСКИЕ РЕКОМЕНДАЦИИ ===
{recommendations}

=== ОБЪЕКТЫ С ПРОБЛЕМАМИ ({batch_info}) ===
{objects_json}

=== НАЙДЕННЫЕ ПРОБЛЕМЫ ===
{issues_text}

=== ПРАВИЛА ===
1. Верни ровно {n_objects} объектов — тот же порядок.
2. НЕ удаляй и НЕ переименовывай поля.
3. Исправляй только поля из списка проблем.
4. Заполняй null ТОЛЬКО данными из рекомендаций выше.
5. Если данных нет — оставь как есть.

Верни ТОЛЬКО валидный JSON (без markdown):
{{
  "corrected_objects": [ ... {n_objects} объектов ... ],
  "changelog": [
    {{"method": "...", "field": "...", "old_value": "...", "new_value": "...", "reason": "..."}}
  ]
}}
"""

BATCH_SIZE = 15  # Max objects per LLM call


class CorrectionStage(BasePipelineStage):
    stage_name = "stage4_correction"
    MAX_OUTPUT_TOKENS = 32768

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        original = context["original_data"]
        analysis = context["analysis"]
        object_issues: list[dict] = analysis.get("object_issues", [])

        # ── Step 1: Deterministic fixes (no LLM) ────────────────────────
        corrected = deepcopy(original)
        corrected, det_changelog = _apply_deterministic_fixes(corrected)

        if det_changelog:
            logger.info(
                "CorrectionStage: %d deterministic fixes applied (Cyrillic etc.)",
                len(det_changelog),
            )

        # ── Step 2: Filter issues that still need LLM ────────────────────
        # After deterministic fixes, remove evidence_level Cyrillic issues
        det_fixed_fields = {
            (c["method"], c["field"]) for c in det_changelog
        }

        llm_actionable = []
        for entry in object_issues:
            remaining_issues = [
                iss for iss in entry.get("issues", [])
                if iss.get("severity") in ("critical", "warning")
                and (entry.get("method", ""), iss.get("field", "")) not in det_fixed_fields
            ]
            if remaining_issues:
                llm_actionable.append({
                    **entry,
                    "issues": remaining_issues,
                })

        if not llm_actionable:
            logger.info("CorrectionStage: all issues fixed deterministically, no LLM needed")
            context["corrected_data"] = corrected
            context["changelog"] = det_changelog
            return context

        # ── Step 3: Batch LLM correction ─────────────────────────────────
        is_list = isinstance(corrected, list)
        all_changelog = list(det_changelog)

        # Build (object, index, issues) tuples
        fix_items = []
        for entry in llm_actionable:
            idx = entry.get("object_index")
            if is_list and idx is not None and 0 <= idx < len(corrected):
                fix_items.append((corrected[idx], idx, entry))

        if not fix_items:
            logger.warning("CorrectionStage: no valid indices for LLM — skipping")
            context["corrected_data"] = corrected
            context["changelog"] = all_changelog
            return context

        # Get recommendation text
        rec_text = self._get_recommendation_text(context, [item[0] for item in fix_items])

        # Process in batches
        for batch_start in range(0, len(fix_items), BATCH_SIZE):
            batch = fix_items[batch_start:batch_start + BATCH_SIZE]
            batch_num = batch_start // BATCH_SIZE + 1
            total_batches = (len(fix_items) + BATCH_SIZE - 1) // BATCH_SIZE

            objects_for_llm = [item[0] for item in batch]
            index_map = [item[1] for item in batch]
            batch_issues = [item[2] for item in batch]

            # Format issues
            issues_lines = []
            for entry in batch_issues:
                method = entry.get("method", "?")
                for iss in entry.get("issues", []):
                    sev = iss.get("severity", "info").upper()
                    field = iss.get("field") or "—"
                    desc = iss.get("description", "")
                    issues_lines.append(f"[{sev}] {method} / {field}: {desc}")

            objects_json = json.dumps(objects_for_llm, ensure_ascii=False, indent=2)

            prompt = _PROMPT.format(
                recommendations=rec_text,
                objects_json=objects_json,
                issues_text="\n".join(issues_lines),
                batch_info=f"пакет {batch_num}/{total_batches}, {len(batch)} объектов",
                n_objects=len(batch),
            )

            logger.info(
                "CorrectionStage: LLM batch %d/%d — %d objects",
                batch_num, total_batches, len(batch),
            )

            try:
                result = self._execute_with_retry(prompt)
            except Exception as e:
                logger.error("CorrectionStage: batch %d failed: %s — skipping", batch_num, e)
                continue

            llm_objects = result.get("corrected_objects")

            if not isinstance(llm_objects, list) or len(llm_objects) != len(batch):
                logger.warning(
                    "CorrectionStage: batch %d — LLM returned %s (expected list[%d]) — skipping",
                    batch_num,
                    f"list[{len(llm_objects)}]" if isinstance(llm_objects, list) else type(llm_objects).__name__,
                    len(batch),
                )
                continue

            # Merge batch results
            pristine = context.get("pristine_original", corrected)
            for pos, orig_idx in enumerate(index_map):
                llm_obj = llm_objects[pos]
                # Guard: restore dropped fields
                ref_obj = pristine[orig_idx] if isinstance(pristine, list) and orig_idx < len(pristine) else corrected[orig_idx]
                for key in ref_obj:
                    if key not in llm_obj:
                        llm_obj[key] = ref_obj[key]
                corrected[orig_idx] = llm_obj

            all_changelog.extend(result.get("changelog", []))

        context["corrected_data"] = corrected
        context["changelog"] = all_changelog

        logger.info(
            "CorrectionStage: total %d changelog entries (%d deterministic + %d LLM)",
            len(all_changelog), len(det_changelog), len(all_changelog) - len(det_changelog),
        )
        return context

    def _get_recommendation_text(self, context: dict, objects: list[dict]) -> str:
        """Extract relevant recommendation text for the given objects."""
        rec_text = context.get("recommendations_full_text", "")

        if context.get("recommendation_chunks") and len(rec_text) > 15_000:
            relevant_sections = set()
            for obj in objects:
                s = obj.get("source_section") or ""
                if s:
                    relevant_sections.add(s.lower().strip())

            if relevant_sections:
                chunks = context["recommendation_chunks"]
                relevant_texts = []
                for chunk in chunks:
                    if any(sec in chunk.text.lower() for sec in relevant_sections):
                        relevant_texts.append(chunk.text)
                if relevant_texts:
                    rec_text = "\n\n---\n\n".join(relevant_texts)

        if len(rec_text) > 20_000:
            rec_text = rec_text[:20_000]

        return rec_text