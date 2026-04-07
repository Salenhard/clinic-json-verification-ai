"""Stage 4 — Correct and supplement the JSON using LLM.

KEY RULES enforced by the prompt:
  • Output must have exactly the same top-level structure as the input.
  • Only existing fields can be filled / corrected.
  • No keys may be added or deleted.
  • All added values must come from the clinical guidelines.
"""
import json
import logging
from copy import deepcopy
from typing import Dict, Any

from .base import BasePipelineStage, PipelineError

logger = logging.getLogger(__name__)


class CorrectionStage(BasePipelineStage):
    stage_name = "stage4_correction"
    MAX_OUTPUT_TOKENS = 32768

    _PROMPT = """\
Задача: исправить и дополнить JSON-документ согласно клиническим рекомендациям.

ОРИГИНАЛЬНЫЙ JSON:
{original_json}

КЛИНИЧЕСКИЕ РЕКОМЕНДАЦИИ (фрагмент):
{recommendations}

НАЙДЕННЫЕ ПРОБЛЕМЫ:
{issues_text}

СТРОГИЕ ПРАВИЛА:
1. Верни документ с ТОЧНО ТАКОЙ ЖЕ СТРУКТУРОЙ — те же ключи верхнего уровня.
2. НЕ удаляй существующие поля.
3. НЕ переименовывай ключи.
4. Только заполняй пустые/null значения или исправляй неверные — согласно рекомендациям.
5. Все добавленные данные должны строго соответствовать клиническим рекомендациям.
6. Если исправить нечего — верни оригинальный JSON без изменений.
Ты можешь добавить отсутствующие сущности из документа.
Верни **полный** исправленный JSON + changelog:
{{
  "corrected_json": {{ ... полный исправленный документ ... }},
  "changelog": [
    {{"action": "added|modified", "field": "путь.к.полю", "old_value": "...", "new_value": "...", "reason": "..."}}
  ]
}}
"""

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        original = context["original_data"]
        analysis = context["analysis"]
        issues = analysis.get("issues", [])
        missing = analysis.get("missing_fields", [])

        # Skip LLM call if nothing to fix
        critical_or_warning = [i for i in issues if i.get("severity") in ("critical", "warning")]
        if not critical_or_warning and not missing:
            logger.info("CorrectionStage: no issues — skipping LLM call")
            context["corrected_data"] = deepcopy(original)
            context["changelog"] = []
            return context

        # Format issues for the prompt
        issues_lines = []
        for iss in issues:
            sev = iss.get("severity", "info").upper()
            field = iss.get("field") or "—"
            desc = iss.get("description", "")
            sugg = iss.get("suggestion", "")
            issues_lines.append(f"[{sev}] {field}: {desc}")
            if sugg:
                issues_lines.append(f"  → {sugg}")
        for f in missing:
            issues_lines.append(f"[MISSING] {f}: поле отсутствует в документе")

        original_json = json.dumps(original, ensure_ascii=False, indent=2)

        rec_text = context.get("recommendations_full_text", "")
        if not rec_text and context.get("recommendation_chunks"):
            rec_text = context["recommendation_chunks"][0].text
        if len(rec_text) > 15_000:
            rec_text = rec_text[:15_000]

        prompt = self._PROMPT.format(
            original_json=original_json,
            recommendations=rec_text,
            issues_text="\n".join(issues_lines) or "Проблем не найдено.",
        )

        result = self._execute_with_retry(prompt)

        corrected = result.get("corrected_json")

        # FIX: документ может быть списком (list) или словарём (dict).
        # Раньше стояла проверка isinstance(corrected, dict), которая всегда
        # отклоняла списки и откатывала к оригиналу — исправления не применялись.
        if not isinstance(corrected, (dict, list)):
            logger.warning(
                "CorrectionStage: LLM вернул некорректный corrected_json (тип %s) — используем оригинал",
                type(corrected).__name__,
            )
            corrected = deepcopy(original)

        # Guard: для dict-документов восстанавливаем выпавшие ключи верхнего уровня.
        # Для list-документов этот guard неприменим — структура проверяется по длине.
        if isinstance(original, dict) and isinstance(corrected, dict):
            for key in original:
                if key not in corrected:
                    logger.warning(
                        "CorrectionStage: LLM удалил ключ '%s' — восстанавливаем из оригинала", key
                    )
                    corrected[key] = original[key]
        elif isinstance(original, list) and isinstance(corrected, list):
            if len(corrected) != len(original):
                logger.warning(
                    "CorrectionStage: LLM изменил длину списка (%d → %d) — используем оригинал",
                    len(original),
                    len(corrected),
                )
                corrected = deepcopy(original)

        context["corrected_data"] = corrected
        context["changelog"] = result.get("changelog", [])

        logger.info("CorrectionStage: применено %d изменений", len(context["changelog"]))
        return context