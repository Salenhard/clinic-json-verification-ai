"""Stage 2 — Analyse JSON against clinical guidelines.

Для каждого чанка рекомендаций фильтрует только релевантные объекты документа
(по source_section / source_number / method), формирует патч supplement_json.

LLM возвращает ТОЛЬКО supplement_json:
  {
    "supplement_json": {
      "updates":   [{"match": {"method": "..."}, "changes": {...}}],
      "additions": [{ ...полная запись... }]
    }
  }

CorrectionStage применяет патч напрямую — без LLM-вызова.
"""
import concurrent.futures
import json
import logging
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

from .base import BasePipelineStage
from .chunker import Chunk

logger = logging.getLogger(__name__)


def _filter_objects_for_chunk(
    objects: List[dict], chunk_text: str
) -> Tuple[List[dict], List[int]]:
    """
    Возвращает (отфильтрованные объекты, исходные индексы).
    Объект считается релевантным если source_section / source_number / method
    встречается в тексте чанка.
    Fallback — пустой список (не все объекты): чанк всё равно запускается
    для генерации additions на основе текста рекомендаций.
    """
    chunk_lower = chunk_text.lower()
    filtered: List[dict] = []
    indices: List[int] = []

    for idx, obj in enumerate(objects):
        if not isinstance(obj, dict):
            continue
        section = (obj.get("source_section") or "").strip().lower()
        number  = (obj.get("source_number")  or "").strip()
        method  = (obj.get("method")          or "").strip().lower()

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

    return filtered, indices


class AnalysisStage(BasePipelineStage):
    stage_name = "stage2_analysis"
    MAX_OUTPUT_TOKENS = 32768
    CHUNK_WORKERS = 3

    # Ключ для идентификации записей (можно переопределить в подклассе)
    RECORD_ID_KEY = "method"

    # Шаблон промпта. Плейсхолдеры:
    #   {id_key}, {json_data}, {chunk_index}, {total_chunks},
    #   {chunk_text}, {existing_methods}
    # Двойные {{ }} — литеральные фигурные скобки.
    PROMPT_TEMPLATE = """\
Задача: проверить JSON-документ на соответствие клиническим рекомендациям \
и сформировать СТРУКТУРИРОВАННЫЙ патч для его дополнения.

JSON-ДОКУМЕНТ (записи релевантные данному фрагменту рекомендаций):
{json_data}

ФРАГМЕНТ КЛИНИЧЕСКИХ РЕКОМЕНДАЦИЙ (часть {chunk_index} из {total_chunks}):
{chunk_text}

ИНСТРУКЦИЯ:
Сгенерируй supplement_json строго в формате ниже.
- updates: изменения в СУЩЕСТВУЮЩИХ записях из JSON-ДОКУМЕНТА выше.
  match — объект для поиска записи по полю "{id_key}" (точное совпадение).
  changes — поля которые нужно добавить или исправить согласно рекомендациям.
- additions: НОВЫЕ записи которых НЕТ в списке СУЩЕСТВУЮЩИХ ЗАПИСЕЙ ниже.
  Каждая запись должна иметь те же поля что и записи в JSON-ДОКУМЕНТЕ.

ПРАВИЛА:
1. В updates.match используй ТОЛЬКО значения "{id_key}" из JSON-ДОКУМЕНТА выше.
2. В changes указывай поля с проблемами (null / пустые / некорректные / неполные).
3. В additions добавляй ТОЛЬКО записи которых нет в СУЩЕСТВУЮЩИХ ЗАПИСЯХ.
4. Все значения строго из клинических рекомендаций — не придумывай.
5. НЕ добавляй поле если оно семантически не применимо к записи \
(например fracture_class не применимо к записям о вывихах без перелома).
6. Если поле уже заполнено корректным значением — не включай его в changes.

СУЩЕСТВУЮЩИЕ ЗАПИСИ В ДОКУМЕНТЕ (полный список "{id_key}" — НЕ добавляй их повторно):
{existing_methods}

Верни ТОЛЬКО валидный JSON (без markdown) — только ключ supplement_json:
{{
  "supplement_json": {{
    "updates": [
      {{"match": {{"{id_key}": "точное название из JSON-ДОКУМЕНТА"}},
       "changes": {{"поле": "новое значение согласно рекомендациям"}}}}
    ],
    "additions": [
      {{"{id_key}": "Название нового метода", "...": "..."}}
    ]
  }}
}}"""

    # ── Prompt builder ────────────────────────────────────────────────────────

    def _build_prompt(
        self,
        chunk_text: str,
        chunk_index: int,
        total_chunks: int,
        json_data: str,
        existing_methods: str,
    ) -> str:
        return self.PROMPT_TEMPLATE.format(
            id_key=self.RECORD_ID_KEY,
            json_data=json_data,
            chunk_index=chunk_index + 1,
            total_chunks=total_chunks,
            chunk_text=chunk_text,
            existing_methods=existing_methods,
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

        id_key = AnalysisStage.RECORD_ID_KEY
        existing_ids = {r.get(id_key) for r in merged_additions}
        for rec in (b.get("additions") or []):
            if rec.get(id_key) not in existing_ids:
                existing_ids.add(rec.get(id_key))
                merged_additions.append(rec)

        return {"updates": merged_updates, "additions": merged_additions}

    @staticmethod
    def _normalize_supplement(raw: Any) -> dict:
        if not isinstance(raw, dict):
            return {"updates": [], "additions": []}
        if "updates" in raw or "additions" in raw:
            return {
                "updates": raw.get("updates") or [],
                "additions": raw.get("additions") or [],
            }
        logger.warning("AnalysisStage: supplement_json без updates/additions — пропускаем")
        return {"updates": [], "additions": []}

    @staticmethod
    def _merge_results(results: List[dict]) -> dict:
        merged: dict = {"updates": [], "additions": []}
        for r in results:
            norm = AnalysisStage._normalize_supplement(r.get("supplement_json"))
            merged = AnalysisStage._merge_supplements(merged, norm)
        return {"supplement_json": merged}

    # ── Per-chunk processor ───────────────────────────────────────────────────

    def _process_chunk(
        self,
        chunk: Chunk,
        all_objects: List[dict],
        existing_methods: str,
        total_chunks: int,
    ) -> Optional[dict]:
        """Фильтрует объекты для чанка, строит промпт, вызывает LLM."""
        filtered, _ = _filter_objects_for_chunk(all_objects, chunk.text)

        # Убираем служебный _object_index перед передачей в LLM
        clean = [
            {k: v for k, v in obj.items() if k != "_object_index"}
            for obj in filtered
        ]

        json_data = json.dumps(clean, ensure_ascii=False, indent=2) if clean else "[]"

        logger.info(
            "%s: chunk %d/%d — %d objects (of %d), %d chars",
            self.stage_name, chunk.index + 1, total_chunks,
            len(filtered), len(all_objects), chunk.char_count,
        )

        prompt = self._build_prompt(
            chunk.text, chunk.index, total_chunks, json_data, existing_methods
        )

        try:
            return self._execute_with_retry(prompt)
        except Exception as e:
            logger.error("%s chunk %d failed: %s", self.stage_name, chunk.index, e)
            return None

    # ── Stage entry point ─────────────────────────────────────────────────────

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        data = context["original_data"]
        all_objects: List[dict] = data if isinstance(data, list) else [data]
        chunks: List[Chunk] = context["recommendation_chunks"]

        # Компактный список всех существующих method-ов для промпта
        existing_methods = "\n".join(
            f"  - {r.get(self.RECORD_ID_KEY, '')}"
            for r in all_objects
            if isinstance(r, dict) and r.get(self.RECORD_ID_KEY)
        )

        # Параллельная обработка чанков
        workers = min(self.CHUNK_WORKERS, len(chunks))
        chunk_results: List[Optional[dict]] = [None] * len(chunks)

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    self._process_chunk,
                    chunk, all_objects, existing_methods, len(chunks),
                ): chunk
                for chunk in chunks
            }
            for fut in concurrent.futures.as_completed(futures):
                chunk = futures[fut]
                chunk_results[chunk.index] = fut.result()

        valid_results = [r for r in chunk_results if r is not None]
        if not valid_results:
            raise Exception(f"{self.stage_name}: все чанки завершились с ошибкой")

        merged = self._merge_results(valid_results)
        supplement = merged.get("supplement_json", {"updates": [], "additions": []})
        context["analysis"] = {"supplement_json": supplement}

        logger.info(
            "Analysis: supplement(updates=%d, additions=%d)  chunks=%d/%d",
            len(supplement.get("updates", [])),
            len(supplement.get("additions", [])),
            len(valid_results), len(chunks),
        )
        return context
