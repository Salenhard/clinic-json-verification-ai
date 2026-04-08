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
    
        # сбор
        for r in results:
            all_actions.extend(r.get("actions", []))
            all_missing.update(r.get("missing_fields", []))
            all_invalid.extend(r.get("invalid_fields", []))
    
            score = r.get("coverage_score")
            if score is not None:
                scores.append(float(score))
    
        # --- ACTIONS: дедуп + разрешение конфликтов ---
        actions_map = {}
    
        for act in all_actions:
            key = (act.get("type"), act.get("path"))
    
            # при конфликте берём последнее (самое "свежее")
            actions_map[key] = act
    
        unique_actions = list(actions_map.values())
    
        # --- INVALID: дедуп ---
        seen_invalid = set()
        unique_invalid = []
    
        for inv in all_invalid:
            key = (inv.get("path"), inv.get("reason"))
            if key not in seen_invalid:
                seen_invalid.add(key)
                unique_invalid.append(inv)
    
        # --- MISSING: фильтр мусора ---
        # убираем явно невалидные индексы (например > max из actions)
        valid_indices = set()
    
        for act in unique_actions:
            path = act.get("path", "")
            if path.startswith("["):
                try:
                    idx = int(path.split("]")[0][1:])
                    valid_indices.add(idx)
                except:
                    pass
    
        filtered_missing = []
    
        for path in all_missing:
            if path.startswith("["):
                try:
                    idx = int(path.split("]")[0][1:])
                    # оставляем только те, что реально есть в данных
                    if idx in valid_indices:
                        filtered_missing.append(path)
                except:
                    pass
            else:
                filtered_missing.append(path)
    
        return {
            "actions": sorted(unique_actions, key=lambda x: x.get("path", "")),
            "missing_fields": sorted(set(filtered_missing)),
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
            "Analysis: score=%.2f  invalid_fields=%d  missing_fields=%d",
            analysis.get("coverage_score", 0),
            len(analysis.get("actions", [])),
            len(analysis.get("invalid_fields", [])),
            len(analysis.get("missing_fields", [])),
        )
        return context
