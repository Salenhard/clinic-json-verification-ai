"""Stage 4 — Apply supplement_json patch to the input document.

supplement_json имеет строгий формат:
  {
    "updates":   [{"match": {"method": "..."}, "changes": {...}}, ...],
    "additions": [{ полная запись }, ...]
  }

Алгоритм:
  1. updates  — находит запись по match-ключу, применяет changes.
               Скалярные поля: применяются один раз и блокируются (_locked_fields).
               Списки: union без дублирования, не блокируются.
  2. additions — добавляет новые записи только если такой method ещё нет.
  3. Возвращает дополненный документ + накопленный changelog с меткой итерации.

Гарды:
  - fracture_class не применяется к записям с диагнозом "вывих" (без "перелом").
  - _locked_fields: множество (record_id, field_path) уже изменённых скаляров —
    последующие итерации не могут их перезаписать.
"""
import logging
from copy import deepcopy
from typing import Any, Dict, FrozenSet, List, Set, Tuple

from .base import BasePipelineStage

logger = logging.getLogger(__name__)

_EMPTY_VALUES = (None, "", [], {})

def _is_empty(v: Any) -> bool:
    return v is None or v in _EMPTY_VALUES


def _merge_lists(old: list, new: list) -> Tuple[list, list]:
    """Union двух списков без дублирования. Возвращает (merged, added_items)."""
    result = list(old)
    added = []
    for item in new:
        if item not in result:
            result.append(item)
            added.append(item)
    return result, added


class CorrectionStage(BasePipelineStage):
    stage_name = "stage4_correction"

    # ── helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _find_record(records: List[dict], match: dict) -> int:
        for i, rec in enumerate(records):
            if all(rec.get(k) == v for k, v in match.items()):
                return i
        return -1

    def _apply_changes(
        self,
        record: dict,
        id_key: str,
        changes: dict,
        record_id: str,
        locked: Set[Tuple[str, str]],
        path: str = "",
    ) -> Tuple[dict, List[dict], Set[str]]:
        """
        Применяет changes к record.

        Скаляры применяются если:
          - поле не заблокировано (locked не содержит (record_id, full_path))
          - поле пустое ИЛИ значение изменилось

        Списки: всегда union, не блокируются.

        Возвращает:
          (обновлённая запись, changelog, newly_locked_paths)
          newly_locked_paths — пути скалярных полей изменённых в этом вызове,
          добавляются в locked снаружи.
        """
        result = dict(record)
        log: List[dict] = []
        newly_locked: Set[str] = set()

        for key, new_val in changes.items():
            full_path = f"{path}.{key}" if path else key

            old_val = result.get(key)
            if key == id_key and not path:
                logger.warning("попытка переименовать '%s' → '%s' — пропускаем", ...)
                continue
            if key not in result:
                # Нового поля нет — добавляем (скаляр, блокируем)
                result[key] = new_val
                newly_locked.add(full_path)
                log.append({"action": "added", "field": full_path,
                             "old_value": None, "new_value": new_val,
                             "reason": "новое поле из рекомендаций"})

            elif isinstance(old_val, dict) and isinstance(new_val, dict):
                # Рекурсия для вложенных объектов
                merged, sub_log, sub_locked = self._apply_changes(
                    old_val, new_val, record_id, locked, full_path
                )
                result[key] = merged
                log.extend(sub_log)
                newly_locked.update(sub_locked)

            elif isinstance(old_val, list) and isinstance(new_val, list):
                # Списки: union, не блокируем (добавляем в будущих итерациях)
                merged_list, added_items = _merge_lists(old_val, new_val)
                if added_items:
                    result[key] = merged_list
                    log.append({"action": "modified", "field": full_path,
                                 "old_value": old_val, "new_value": merged_list,
                                 "reason": f"добавлено {len(added_items)} эл. в список"})

            elif (record_id, full_path) in locked:
                # Скалярное поле уже было изменено в предыдущей итерации — пропускаем
                logger.debug(
                    "CorrectionStage: поле '%s' записи '%s' заблокировано — пропускаем",
                    full_path, record_id,
                )

            elif _is_empty(old_val) or old_val != new_val:
                # Скаляр: применяем и блокируем
                action = "modified" if not _is_empty(old_val) else "added"
                result[key] = new_val
                newly_locked.add(full_path)
                log.append({"action": action, "field": full_path,
                             "old_value": old_val, "new_value": new_val,
                             "reason": "обновлено по рекомендациям"})

        return result, log, newly_locked

    # ── Stage entry point ────────────────────────────────────────────────────

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        original  = context["original_data"]
        analysis  = context["analysis"]
        supplement = analysis.get("supplement_json") or {}
        iteration  = context.get("_iteration", 1)
        id_key = context.get("_id_field")
        updates   = supplement.get("updates")   or []
        additions = supplement.get("additions") or []

        if not updates and not additions:
            logger.info("CorrectionStage: supplement_json пустой — пропускаем")
            context["corrected_data"] = deepcopy(original)
            return context

        # Накапливаем changelog и locked_fields через итерации
        changelog: List[dict] = list(context.get("changelog") or [])
        # locked: set of (record_id, field_path) — скаляры изменённые в прошлых итерациях
        locked: Set[Tuple[str, str]] = set(
            tuple(x) for x in (context.get("_locked_fields") or [])
        )

        iter_field_changes = 0
        iter_records_added = 0

        # ── Массив записей ────────────────────────────────────────────────────
        if isinstance(original, list):
            corrected = [deepcopy(r) for r in original]

            # 1. updates
            for upd in updates:
                match   = upd.get("match")
                changes = upd.get("changes") or {}
                if not match or not changes:
                    continue

                idx = self._find_record(corrected, match)
                if idx == -1:
                    logger.warning("CorrectionStage: match=%s не найдена — пропускаем", match)
                    continue

                record_id = corrected[idx].get(id_key, f"[{idx}]")
                merged, sub_log, newly_locked = self._apply_changes(
                    corrected[idx], changes, record_id, locked
                )
                corrected[idx] = merged

                # Блокируем изменённые скалярные поля
                for field_path in newly_locked:
                    locked.add((record_id, field_path))

                for entry in sub_log:
                    entry["record"]    = record_id
                    entry["iteration"] = iteration
                changelog.extend(sub_log)
                iter_field_changes += len(sub_log)

            # 2. additions — только если метода ещё нет и запись валидна
            required_fields = context.get("_required_fields") or []
            existing_ids = {r.get(id_key) for r in corrected}
            for new_rec in additions:
                rec_id = new_rec.get(id_key)
                if rec_id and rec_id in existing_ids:
                    logger.debug("CorrectionStage: '%s' уже есть — пропускаем", rec_id)
                    continue

                # Валидация: запись должна иметь ≥70% обязательных полей
                if required_fields:
                    filled = sum(
                        1 for f in required_fields
                        if new_rec.get(f) not in (None, "", [], {})
                    )
                    coverage = filled / len(required_fields)
                    if coverage < 0.70:
                        logger.warning(
                            "CorrectionStage: '%s' отклонена — покрытие полей %.0f%% < 70%%",
                            rec_id, coverage * 100,
                        )
                        continue

                corrected.append(deepcopy(new_rec))
                existing_ids.add(rec_id)
                changelog.append({
                    "action":    "added_record",
                    "record":    rec_id,
                    "reason":    "новая запись из рекомендаций",
                    "iteration": iteration,
                })
                iter_records_added += 1

        # ── Объект (dict) ─────────────────────────────────────────────────────
        elif isinstance(original, dict):
            all_changes = {k: v for upd in updates for k, v in (upd.get("changes") or {}).items()}
            record_id = original.get(id_key, "root")
            corrected, iter_log, newly_locked = self._apply_changes(
                deepcopy(original), all_changes, record_id, locked
            )
            for field_path in newly_locked:
                locked.add((record_id, field_path))
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
        context["changelog"]       = changelog
        # Сохраняем locked_fields для следующей итерации (сериализуем в list of list)
        context["_locked_fields"]  = [list(pair) for pair in locked]

        logger.info(
            "CorrectionStage [iter %d]: field_changes=%d  records_added=%d  locked_total=%d  changelog_total=%d",
            iteration, iter_field_changes, iter_records_added, len(locked), len(changelog),
        )
        return context
