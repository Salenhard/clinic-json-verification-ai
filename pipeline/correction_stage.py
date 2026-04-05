from .base import BasePipelineStage, PipelineError
from copy import deepcopy
import json
from typing import Dict, Any

class CorrectionStage(BasePipelineStage):
    stage_name = "stage4_correction"
    MAX_OUTPUT_TOKENS = 32768

    _PROMPT = """\
Задача: исправить и дополнить JSON согласно клиническим рекомендациям.

ОРИГИНАЛЬНЫЙ JSON:
{original_json}

РЕКОМЕНДАЦИИ:
{recommendations}

НАЙДЕННЫЕ ПРОБЛЕМЫ:
{issues_text}

Ты можешь:
- Исправлять существующие поля
- Добавлять отсутствующие поля (missing_fields)
- Заполнять значения по рекомендациям

НЕ МЕНЯЙ СТРУКТУРУ ИСХОДНОГО JSON (не удаляй поля, не переименовывай ключи верхнего уровня).

Верни **полный** исправленный JSON + changelog:
{{
  "corrected_json": {{ ... полный JSON ... }},
  "changelog": [
    {{"action": "added|modified", "field": "имя_поля", "reason": "..." }}
  ]
}}
"""

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        corrected = deepcopy(context["original_data"])
        suggestions = context["analysis"].get("suggestions", {})

        for field, value in suggestions.items():
            corrected[field] = value
              
        for field in context["analysis"].get("missing_fields", []):
            if field not in corrected:
                corrected[field] = None

        context["corrected_data"] = corrected
        return context