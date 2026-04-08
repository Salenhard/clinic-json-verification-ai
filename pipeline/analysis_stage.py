"""Stage 2 — Analyse JSON against clinical guidelines.

Processes guidelines in chunks (for large PDFs) and merges results.
"""
import json
import logging
from typing import Dict, Any

from .base import BasePipelineStage, PipelineError

logger = logging.getLogger(__name__)


class AnalysisStage(BasePipelineStage):
    stage_name = "stage2_analysis"
    MAX_OUTPUT_TOKENS = 32768

    # FIX: renamed to _PROMPT_TEMPLATE to match usage; was _PROMPT (unused variable)
    _PROMPT_TEMPLATE = """\
ЗАДАЧА:
Провести структурную проверку JSON-документа на соответствие клиническим рекомендациям и выдать ТОЛЬКО машинно-применимые инструкции для исправления.

ВХОДНЫЕ ДАННЫЕ:
1. JSON-ДОКУМЕНТ:
{json_data}

=== ФРАГМЕНТ РЕКОМЕНДАЦИЙ (часть {chunk_index} из {total_chunks}) ===
{chunk_text}

ЧТО НУЖНО СДЕЛАТЬ:
Проанализируй JSON и сравни с рекомендациями. Определи:
- отсутствующие обязательные поля
- некорректные значения
- противоречия
- неполное покрытие данных
- лишние или недопустимые поля

ФОРМАТ ВЫХОДА:
Верни ТОЛЬКО валидный JSON без пояснений.

Результат должен содержать ТОЛЬКО действия, которые можно автоматически применить:

{{
  "actions": [
    {{
      "type": "add",
      "path": "путь.к.полю",
      "value": "значение"
    }},
    {{
      "type": "update",
      "path": "путь.к.полю",
      "old_value": "старое значение",
      "new_value": "новое значение"
    }},
    {{
      "type": "remove",
      "path": "путь.к.полю"
    }}
  ],
  "missing_fields": [
    "путь.к.полю"
  ],
  "invalid_fields": [
    {{
      "path": "путь.к.полю",
      "reason": "конкретная причина"
    }}
  ],
  "coverage_score": 0.0,
  "overall_comment": "краткий итог без общих рассуждений"
}}

ТРЕБОВАНИЯ К ДЕЙСТВИЯМ:
- path указывать в формате JSONPath (например: patient.age, diagnosis.code)
- value должен быть конкретным (не "указать", а реальное значение)
- не дублировать действия
- если поле отсутствует → только add
- если поле есть, но неверное → только update
- если поле лишнее → remove
- не писать текст вне JSON

ЗАПРЕЩЕНО:
- ссылки на рекомендации
- объяснения
- размытые формулировки
- дублирование
"""
    def _build_prompt(self, chunk_text: str, chunk_index: int, total_chunks: int, json_data: str) -> str:
        return self._PROMPT_TEMPLATE.format(
            json_data=json_data,
            chunk_text=chunk_text,
            chunk_index=chunk_index + 1,
            total_chunks=total_chunks,
        )

    @staticmethod
    def _merge_results(results: list) -> dict:
        all_actions = []
        all_missing: set[str] = set()
        all_invalid = []
        scores = []

        # Сбор данных
        for r in results:
            all_actions.extend(r.get("actions", []))
            all_missing.update(r.get("missing_fields", []))
            all_invalid.extend(r.get("invalid_fields", []))

            score = r.get("coverage_score")
            if score is not None:
                scores.append(float(score))

        # Дедупликация actions
        seen_actions: set[tuple] = set()
        unique_actions = []

        for act in all_actions:
            key = (
                act.get("type"),
                act.get("path"),
                str(act.get("value")),
                str(act.get("new_value"))
            )
            if key not in seen_actions:
                seen_actions.add(key)
                unique_actions.append(act)

        # Дедупликация invalid_fields
        seen_invalid: set[tuple] = set()
        unique_invalid = []

        for inv in all_invalid:
            key = (inv.get("path"), inv.get("reason"))
            if key not in seen_invalid:
                seen_invalid.add(key)
                unique_invalid.append(inv)

        return {
            "actions": unique_actions,
            "missing_fields": sorted(all_missing),
            "invalid_fields": unique_invalid,
            "coverage_score": round(sum(scores) / len(scores), 3) if scores else 0.0,
            "overall_comment": f"Проанализировано {len(results)} фрагментов рекомендаций."
        }

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        json_data = json.dumps(context["original_data"], ensure_ascii=False, indent=2)

        chunks = context["recommendation_chunks"]

        analysis = self._execute_over_chunks(
            chunks=chunks,
            build_prompt_fn=lambda text, idx, total: self._build_prompt(text, idx, total, json_data),
            merge_fn=self._merge_results,
        )

        context["analysis"] = analysis
        logger.info(
            "Analysis: score=%.2f  issues=%d  missing_fields=%d",
            analysis.get("completeness_score", 0),
            len(analysis.get("actions", [])),
            len(analysis.get("missing_fields", [])),
            len(analysis.get("invalid_fields", [])),
        )
        return context
