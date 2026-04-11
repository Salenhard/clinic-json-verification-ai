"""Stage 4 — Apply supplement_json patch to the input document.

supplement_json имеет строгий формат:
  {
    "updates":   [{"match": {"method": "..."}, "changes": {...}}, ...],
    "additions": [{ полная запись }, ...]
  }

Алгоритм:
  1. updates  — находит запись по match-ключу, делает deep-merge changes.
               Перезаписывает только null/пустые поля, ИЛИ поля из critical issues.
  2. additions — добавляет новые записи (если такой method ещё нет).
  3. Возвращает дополненный документ + changelog.
"""
import logging
from copy import deepcopy
from typing import Any, Dict, List, Set

from .base import BasePipelineStage

logger = logging.getLogger(__name__)

_EMPTY_VALUES = (None, "", [], {})


def _is_empty(v: Any) -> bool:
    return v is None or v in _EMPTY_VALUES


class CorrectionStage(BasePipelineStage):
    stage_name = "stage4_correction"

    ID_KEY = "method"  # ключ для поиска записи в массиве

    # ── helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _critical_fields(issues: List[dict]) -> Set[str]:
        return {
            iss["field"]
            for iss in issues
            if iss.get("severity") == "critical" and iss.get("field")
        }

    @staticmethod
    def _find_record(records: List[dict], match: dict) -> int:
        """Возвращает индекс первой записи, удовлетворяющей всем условиям match."""
        for i, rec in enumerate(records):
            if all(rec.get(k) == v for k, v in match.items()):
                return i
        return -1

    def _merge_changes(
        self,
        record: dict,
        changes: dict,
        critical: Set[str],
        path: str = "",
    ) -> tuple:
        """
        Применяет changes к record.
        Возвращает (обновлённая запись, список changelog-записей).
        """
        result = dict(record)
        log: List[dict] = []

        for key, new_val in changes.items():
            full_path = f"{path}.{key}" if path else key
            old_val = result.get(key)

            if key not in result:
                # Поля не было — добавляем
                result[key] = new_val
                log.append({"action": "added", "field": full_path,
                             "old_value": None, "new_value": new_val,
                             "reason": "новое поле из рекомендаций"})

            elif isinstance(old_val, dict) and isinstance(new_val, dict):
                # Рекурсия для вложенных объектов
                merged, sub_log = self._merge_changes(old_val, new_val, critical, full_path)
                result[key] = merged
                log.extend(sub_log)

            elif _is_empty(old_val):
                # Пустое / null — заполняем
                result[key] = new_val
                log.append({"action": "added", "field": full_path,
                             "old_value": old_val, "new_value": new_val,
                             "reason": "заполнено пустое поле"})

            elif key in critical or full_path in critical:
                # Critical issue — исправляем даже непустое
                result[key] = new_val
                log.append({"action": "modified", "field": full_path,
                             "old_value": old_val, "new_value": new_val,
                             "reason": "исправлено: critical issue"})

            # Иначе — не трогаем корректное непустое значение

        return result, log

    # ── Stage entry point ────────────────────────────────────────────────────

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        original = context["original_data"]
        analysis = context["analysis"]
        supplement = analysis.get("supplement_json") or {}

        updates = supplement.get("updates") or []
        additions = supplement.get("additions") or []

        if not updates and not additions:
            logger.info("CorrectionStage: supplement_json пустой — пропускаем")
            context["corrected_data"] = deepcopy(original)
            context["changelog"] = []
            return context

        # issues больше не генерируются — все изменения из supplement_json применяются
        critical: Set[str] = set()
        changelog: List[dict] = []

        # ── Работаем с массивом записей ──────────────────────────────────────
        if isinstance(original, list):
            corrected = [deepcopy(r) for r in original]

            # 1. Применяем updates
            for upd in updates:
                match = upd.get("match")
                changes = upd.get("changes") or {}
                if not match or not changes:
                    continue

                idx = self._find_record(corrected, match)
                if idx == -1:
                    logger.warning(
                        "CorrectionStage: запись match=%s не найдена — пропускаем", match
                    )
                    continue

                record_id = corrected[idx].get(self.ID_KEY, f"[{idx}]")
                merged, sub_log = self._merge_changes(corrected[idx], changes, critical)
                corrected[idx] = merged
                for entry in sub_log:
                    entry["record"] = record_id
                changelog.extend(sub_log)

            # 2. Добавляем новые записи
            existing_ids = {r.get(self.ID_KEY) for r in corrected}
            for new_rec in additions:
                rec_id = new_rec.get(self.ID_KEY)
                if rec_id and rec_id in existing_ids:
                    logger.debug("CorrectionStage: addition '%s' уже есть — пропускаем", rec_id)
                    continue
                corrected.append(deepcopy(new_rec))
                existing_ids.add(rec_id)
                changelog.append({
                    "action": "added_record",
                    "record": rec_id,
                    "reason": "новая запись из рекомендаций",
                })

        # ── Работаем с объектом (dict) ───────────────────────────────────────
        elif isinstance(original, dict):
            corrected, changelog = self._merge_changes(
                deepcopy(original),
                # Для dict-документа объединяем все changes из updates
                {k: v for upd in updates for k, v in (upd.get("changes") or {}).items()},
                critical,
            )
            # additions для dict не применимы — логируем предупреждение
            if additions:
                logger.warning(
                    "CorrectionStage: документ — dict, %d additions проигнорированы", len(additions)
                )
        else:
            logger.warning("CorrectionStage: неподдерживаемый тип документа — пропускаем")
            corrected = deepcopy(original)

        context["corrected_data"] = corrected
        context["changelog"] = changelog

        logger.info(
            "CorrectionStage: updates=%d  additions=%d  changes=%d",
            len(updates),
            len(additions),
            len(changelog),
        )
        return context
