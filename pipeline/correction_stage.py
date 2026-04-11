"""Stage 4 — Apply supplement_json patch to the input document.

supplement_json имеет строгий формат:
  {
    "updates":   [{"match": {"method": "..."}, "changes": {...}}, ...],
    "additions": [{ полная запись }, ...]
  }

Алгоритм:
  1. updates  — находит запись по match-ключу, применяет ВСЕ changes
               (LLM целенаправленно выбрал запись через match — доверяем).
  2. additions — добавляет новые записи только если такой method ещё нет.
  3. Возвращает дополненный документ + накопленный changelog с меткой итерации.
"""
import logging
from copy import deepcopy
from typing import Any, Dict, List

from .base import BasePipelineStage

logger = logging.getLogger(__name__)

_EMPTY_VALUES = (None, "", [], {})


def _is_empty(v: Any) -> bool:
    return v is None or v in _EMPTY_VALUES


class CorrectionStage(BasePipelineStage):
    stage_name = "stage4_correction"

    ID_KEY = "method"

    # ── helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _find_record(records: List[dict], match: dict) -> int:
        """Возвращает индекс первой записи, удовлетворяющей всем условиям match."""
        for i, rec in enumerate(records):
            if all(rec.get(k) == v for k, v in match.items()):
                return i
        return -1

    def _apply_changes(
        self,
        record: dict,
        changes: dict,
        force: bool = False,
        path: str = "",
    ) -> tuple:
        """
        Применяет changes к record.
        force=True  → перезаписывает любые значения (используется для updates).
        force=False → только null/пустые (используется при merge additions).
        Возвращает (обновлённая запись, список changelog-записей).
        """
        result = dict(record)
        log: List[dict] = []

        for key, new_val in changes.items():
            full_path = f"{path}.{key}" if path else key
            old_val = result.get(key)

            if key not in result:
                result[key] = new_val
                log.append({"action": "added", "field": full_path,
                             "old_value": None, "new_value": new_val,
                             "reason": "новое поле из рекомендаций"})

            elif isinstance(old_val, dict) and isinstance(new_val, dict):
                # Рекурсия для вложенных объектов (наследует force)
                merged, sub_log = self._apply_changes(old_val, new_val, force, full_path)
                result[key] = merged
                log.extend(sub_log)

            elif force:
                # updates: всегда применяем если значение изменилось
                if old_val != new_val:
                    action = "modified" if not _is_empty(old_val) else "added"
                    result[key] = new_val
                    log.append({"action": action, "field": full_path,
                                 "old_value": old_val, "new_value": new_val,
                                 "reason": "обновлено по рекомендациям"})

            elif _is_empty(old_val):
                # additions merge: только пустые поля
                result[key] = new_val
                log.append({"action": "added", "field": full_path,
                             "old_value": old_val, "new_value": new_val,
                             "reason": "заполнено пустое поле"})

        return result, log

    # ── Stage entry point ────────────────────────────────────────────────────

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        original = context["original_data"]
        analysis = context["analysis"]
        supplement = analysis.get("supplement_json") or {}
        iteration = context.get("_iteration", 1)

        updates = supplement.get("updates") or []
        additions = supplement.get("additions") or []

        if not updates and not additions:
            logger.info("CorrectionStage: supplement_json пустой — пропускаем")
            context["corrected_data"] = deepcopy(original)
            return context

        # Накапливаем changelog через итерации
        changelog: List[dict] = list(context.get("changelog") or [])
        iter_field_changes = 0
        iter_records_added = 0

        # ── Массив записей ────────────────────────────────────────────────────
        if isinstance(original, list):
            corrected = [deepcopy(r) for r in original]

            # 1. updates — force=True: применяем все изменения
            for upd in updates:
                match = upd.get("match")
                changes = upd.get("changes") or {}
                if not match or not changes:
                    continue

                idx = self._find_record(corrected, match)
                if idx == -1:
                    logger.warning("CorrectionStage: match=%s не найдена — пропускаем", match)
                    continue

                record_id = corrected[idx].get(self.ID_KEY, f"[{idx}]")
                merged, sub_log = self._apply_changes(corrected[idx], changes, force=True)
                corrected[idx] = merged
                for entry in sub_log:
                    entry["record"] = record_id
                    entry["iteration"] = iteration
                changelog.extend(sub_log)
                iter_field_changes += len(sub_log)

            # 2. additions — только если метода ещё нет
            existing_ids = {r.get(self.ID_KEY) for r in corrected}
            for new_rec in additions:
                rec_id = new_rec.get(self.ID_KEY)
                if rec_id and rec_id in existing_ids:
                    logger.debug("CorrectionStage: '%s' уже есть — пропускаем", rec_id)
                    continue
                corrected.append(deepcopy(new_rec))
                existing_ids.add(rec_id)
                changelog.append({
                    "action": "added_record",
                    "record": rec_id,
                    "reason": "новая запись из рекомендаций",
                    "iteration": iteration,
                })
                iter_records_added += 1

        # ── Объект (dict) ─────────────────────────────────────────────────────
        elif isinstance(original, dict):
            all_changes = {k: v for upd in updates for k, v in (upd.get("changes") or {}).items()}
            corrected, iter_log = self._apply_changes(deepcopy(original), all_changes, force=True)
            for entry in iter_log:
                entry["iteration"] = iteration
            changelog.extend(iter_log)
            iter_field_changes = len(iter_log)
            if additions:
                logger.warning("CorrectionStage: dict-документ, %d additions пропущены", len(additions))
        else:
            logger.warning("CorrectionStage: неподдерживаемый тип — пропускаем")
            corrected = deepcopy(original)

        context["corrected_data"] = corrected
        context["changelog"] = changelog

        logger.info(
            "CorrectionStage [iter %d]: field_changes=%d  records_added=%d  changelog_total=%d",
            iteration, iter_field_changes, iter_records_added, len(changelog),
        )
        return context
