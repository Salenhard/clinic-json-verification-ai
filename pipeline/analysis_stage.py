"""Stage 2 — Analyse JSON against clinical guidelines.

Обрабатывает рекомендации по чанкам, объединяет результаты.

LLM возвращает ТОЛЬКО supplement_json — строго структурированный патч:
  {
    "supplement_json": {
      "updates":   [{"match": {"method": "..."}, "changes": {...}}],
      "additions": [{ ...полная запись... }]
    }
  }

CorrectionStage применяет патч напрямую — без LLM-вызова.
"""
import json
import logging
from typing import Dict, Any, List

from .base import BasePipelineStage

logger = logging.getLogger(__name__)


class AnalysisStage(BasePipelineStage):
    stage_name = "stage2_analysis"
    MAX_OUTPUT_TOKENS = 32768

    # Ключ для идентификации записей в массиве (можно переопределить в подклассе)
    RECORD_ID_KEY = "method"

    # Шаблон промпта. Плейсхолдеры: {id_key}, {json_data}, {chunk_index},
    # {total_chunks}, {chunk_text}.
    # Двойные {{ }} — литеральные фигурные скобки в итоговом тексте.
    PROMPT_TEMPLATE = """\
Задача: проверить JSON-документ на соответствие клиническим рекомендациям \
и сформировать СТРУКТУРИРОВАННЫЙ патч для его дополнения.

JSON-ДОКУМЕНТ:
{json_data}

ФРАГМЕНТ КЛИНИЧЕСКИХ РЕКОМЕНДАЦИЙ (часть {chunk_index} из {total_chunks}):
{chunk_text}

ИНСТРУКЦИЯ:
Сгенерируй supplement_json строго в формате ниже.
- updates: изменения в СУЩЕСТВУЮЩИХ записях.
  match — объект для поиска записи по полю "{id_key}" (точное совпадение).
  changes — только поля которые нужно добавить или исправить.
- additions: НОВЫЕ записи которых нет в документе.
  Каждая запись должна иметь все обязательные поля из схемы.

ПРАВИЛА:
1. В updates.match используй ТОЛЬКО существующие значения из документа.
2. В changes указывай ТОЛЬКО поля с проблемами (null / пустые / некорректные).
3. В additions добавляй ТОЛЬКО записи которых нет в документе.
4. Все значения строго из клинических рекомендаций — не придумывай.
5. Структура additions должна совпадать со структурой документа — используй те же поля что есть в документе.

Верни ТОЛЬКО валидный JSON (без markdown) — только ключ supplement_json:
{{
  "supplement_json": {{
    "updates": [
      {{"match": {{"{id_key}": "точное название из документа"}},
       "changes": {{"поле": "новое значение согласно рекомендациям"}}}}
    ],
    "additions": [
      {{"{id_key}": "Название нового метода", "...": "..."}}
    ]
  }}
}}"""

    # ── Schema hint ───────────────────────────────────────────────────────────

    @staticmethod
    def _build_schema_hint(sample_record: Any) -> str:
        """Формирует подсказку о структуре записи для промпта."""
        if isinstance(sample_record, dict):
            keys = list(sample_record.keys())[:12]
            return f"Поля записи: {keys}"
        if isinstance(sample_record, list) and sample_record:
            keys = list(sample_record[0].keys())[:12] if isinstance(sample_record[0], dict) else []
            return f"Массив записей. Поля каждой записи: {keys}"
        return ""

    # ── Prompt builder ────────────────────────────────────────────────────────

    def _build_prompt(
        self,
        chunk_text: str,
        chunk_index: int,
        total_chunks: int,
        json_data: str,
    ) -> str:
        return self.PROMPT_TEMPLATE.format(
            id_key=self.RECORD_ID_KEY,
            json_data=json_data,
            chunk_index=chunk_index + 1,
            total_chunks=total_chunks,
            chunk_text=chunk_text,
        )

    # ── Merge helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def _deep_merge_dicts(base: dict, override: dict) -> dict:
        result = dict(base)
        for k, v in override.items():
            if k in result and isinstance(result[k], dict) and isinstance(v, dict):
                result[k] = AnalysisStage._deep_merge_dicts(result[k], v)
            elif isinstance(v, list) and isinstance(result.get(k), list):
                existing = list(result[k])
                for item in v:
                    if item not in existing:
                        existing.append(item)
                result[k] = existing
            else:
                result[k] = v
        return result

    @staticmethod
    def _merge_supplements(a: dict, b: dict) -> dict:
        """Объединяет два supplement_json без дублирования."""
        merged_updates: List[dict] = list(a.get("updates") or [])
        merged_additions: List[dict] = list(a.get("additions") or [])

        # updates объединяем по match-ключу — changes мержим
        existing_matches = {
            json.dumps(u.get("match", {}), sort_keys=True): i
            for i, u in enumerate(merged_updates)
        }
        for u in (b.get("updates") or []):
            key = json.dumps(u.get("match", {}), sort_keys=True)
            if key in existing_matches:
                idx = existing_matches[key]
                merged_updates[idx]["changes"] = AnalysisStage._deep_merge_dicts(
                    merged_updates[idx].get("changes", {}),
                    u.get("changes", {}),
                )
            else:
                existing_matches[key] = len(merged_updates)
                merged_updates.append(u)

        # additions объединяем без дублирования по id-ключу
        id_key = AnalysisStage.RECORD_ID_KEY
        existing_ids = {r.get(id_key) for r in merged_additions}
        for rec in (b.get("additions") or []):
            if rec.get(id_key) not in existing_ids:
                existing_ids.add(rec.get(id_key))
                merged_additions.append(rec)

        return {"updates": merged_updates, "additions": merged_additions}

    @staticmethod
    def _normalize_supplement(raw: Any) -> dict:
        """Нормализует supplement_json к {updates, additions}."""
        if not isinstance(raw, dict):
            return {"updates": [], "additions": []}
        if "updates" in raw or "additions" in raw:
            return {
                "updates": raw.get("updates") or [],
                "additions": raw.get("additions") or [],
            }
        logger.warning("AnalysisStage: supplement_json без updates/additions — пропускаем")
        return {"updates": [], "additions": []}

    # ── Chunk results merge ───────────────────────────────────────────────────

    @staticmethod
    def _merge_results(results: list) -> dict:
        merged_supplement: dict = {"updates": [], "additions": []}

        for r in results:
            raw_sup = r.get("supplement_json")
            norm = AnalysisStage._normalize_supplement(raw_sup)
            merged_supplement = AnalysisStage._merge_supplements(merged_supplement, norm)

        return {"supplement_json": merged_supplement}

    # ── Stage entry point ─────────────────────────────────────────────────────

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        data = context["original_data"]
        json_data = json.dumps(data, ensure_ascii=False, indent=2)

        chunks = context["recommendation_chunks"]

        result = self._execute_over_chunks(
            chunks=chunks,
            build_prompt_fn=lambda text, idx, total: self._build_prompt(
                text, idx, total, json_data
            ),
            merge_fn=self._merge_results,
        )

        # Сохраняем supplement_json прямо в context — analysis теперь только патч
        supplement = result.get("supplement_json", {"updates": [], "additions": []})
        context["analysis"] = {"supplement_json": supplement}

        logger.info(
            "Analysis: supplement(updates=%d, additions=%d)",
            len(supplement.get("updates", [])),
            len(supplement.get("additions", [])),
        )
        return context
