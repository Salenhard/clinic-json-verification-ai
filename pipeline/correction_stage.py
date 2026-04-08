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
Задача: исправить JSON-документ, используя ТОЛЬКО отчёт о проблемах.

ОРИГИНАЛЬНЫЙ JSON:
{original_json}

ОТЧЁТ О ПРОБЛЕМАХ:
{issues_text}

СТРОГИЕ ПРАВИЛА:
1. Верни документ с ТОЧНО ТАКОЙ ЖЕ СТРУКТУРОЙ — те же ключи верхнего уровня.
2. НЕ удаляй существующие поля.
3. НЕ переименовывай ключи.
4. НЕ добавляй новые поля, если это не указано в issues как critical.
5. Если исправить нечего — верни оригинальный JSON без изменений.

Верни **полный** исправленный JSON + changelog:

{
  "corrected_json": { ... полный исправленный документ ... },
  "changelog": [
    {
      "action": "added|modified",
      "field": "путь.к.полю",
      "old_value": "...",
      "new_value": "...",
      "reason": "..."
    }
  ]
}
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

        prompt = self._PROMPT.format(
            original_json=original_json,
            issues_text="\n".join(issues_lines) or "Проблем не найдено.",
        )

        result = self._execute_with_retry(prompt)

        # FIX: validate that LLM preserved structure
        corrected = result.get("corrected_json")
        if not isinstance(corrected, dict):
            logger.warning("CorrectionStage: LLM returned invalid corrected_json — using original")
            corrected = deepcopy(original)

        # Guard: ensure no top-level keys were dropped
        for key in original:
            if key not in corrected:
                logger.warning("CorrectionStage: LLM dropped key '%s' — restoring from original", key)
                corrected[key] = original[key]

        context["corrected_data"] = corrected
        context["changelog"] = result.get("changelog", [])

        logger.info("CorrectionStage: %d changes applied", len(context["changelog"]))
        return context
