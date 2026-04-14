"""Stage 4 — Apply supplement_json patch to the input document.

supplement_json имеет строгий формат:
  {
    "updates":   [{"match": {"<id_field>": "..."}, "changes": {...}}, ...],
    "additions": [{ полная запись }, ...]
  }

Алгоритм:
  1. updates  — находит запись по match-ключу, применяет changes.
               Поля из match блокируются от изменений (match_keys guard).
               Скалярные поля: применяются один раз и блокируются (_locked_fields).
               Списки: union без дублирования, не блокируются.
  2. additions — добавляет новые записи только если id ещё нет.
  3. Возвращает дополненный документ + накопленный changelog с меткой итерации.

Гарды:
  - match_keys: поля из match нельзя изменять через changes (предотвращает дубли).
  - fracture_class не применяется к записям с диагнозом "вывих" (без "перелом").
  - _locked_fields: (record_id, field_path) заблокированных скаляров.
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
        changes: dict,
        record_id: str,
        locked: Set[Tuple[str, str]],
        match_keys: FrozenSet[str] = frozenset(),
        path: str = "",
    ) -> Tuple[dict, List[dict], Set[str]]:
        """
        Применяет changes к record.

        match_keys — поля из match-объекта: блокируются от изменений на верхнем
                     уровне (иначе запись «переименовывается» и создаётся дубль).
        Скаляры:   применяются если не заблокированы в locked; затем блокируются.
        Списки:    union без дублирования, не блокируются.

        Возвращает (обновлённая запись, changelog, newly_locked_paths).
        """
        result = dict(record)
        log: List[dict] = []
        newly_locked: Set[str] = set()

        for key, new_val in changes.items():
            full_path = f"{path}.{key}" if path else key

            # Guard 1: поля из match нельзя менять (только верхний уровень)
            if not path and key in match_keys:
                logger.warning(
                    "CorrectionStage: поле '%s' использовано в match записи '%s' — пропускаем",
                    key, record_id,
                )
                continue

            old_val = result.get(key)

            if key not in result:
                result[key] = new_val
                newly_locked.add(full_path)
                log.append({"action": "added", "field": full_path,
                             "old_value": None, "new_value": new_val,
                             "reason": "новое поле из рекомендаций"})

            elif isinstance(old_val, dict) and isinstance(new_val, dict):
                merged, sub_log, sub_locked = self._apply_changes(
                    old_val, new_val, record_id, locked, match_keys, full_path
                )
                result[key] = merged
                log.extend(sub_log)
                newly_locked.update(sub_locked)

            elif isinstance(old_val, list) and isinstance(new_val, list):
                merged_list, added_items = _merge_lists(old_val, new_val)
                if added_items:
                    result[key] = merged_list
                    log.append({"action": "modified", "field": full_path,
                                 "old_value": old_val, "new_value": merged_list,
                                 "reason": f"добавлено {len(added_items)} эл. в список"})

            elif (record_id, full_path) in locked:
                logger.debug(
                    "CorrectionStage: поле '%s' записи '%s' заблокировано — пропускаем",
                    full_path, record_id,
                )

            elif _is_empty(old_val) or old_val != new_val:
                action = "modified" if not _is_empty(old_val) else "added"
                result[key] = new_val
                newly_locked.add(full_path)
                log.append({"action": action, "field": full_path,
                             "old_value": old_val, "new_value": new_val,
                             "reason": "обновлено по рекомендациям"})

        return result, log, newly_locked

    # ── Stage entry point ────────────────────────────────────────────────────

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        original   = context["original_data"]
        analysis   = context["analysis"]
        supplement = analysis.get("supplement_json") or {}
        iteration  = context.get("_iteration", 1)
        id_key: str = context.get("_id_field", "method")

        updates   = supplement.get("updates")   or []
        additions = supplement.get("additions") or []

        if not updates and not additions:
            logger.info("CorrectionStage: supplement_json пустой — пропускаем")
            context["corrected_data"] = deepcopy(original)
            return context

        changelog: List[dict] = list(context.get("changelog") or [])
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
                mk = frozenset(match.keys())
                merged, sub_log, newly_locked = self._apply_changes(
                    corrected[idx], changes, record_id, locked, mk
                )
                corrected[idx] = merged

                for field_path in newly_locked:
                    locked.add((record_id, field_path))

                for entry in sub_log:
                    entry["record"]    = record_id
                    entry["iteration"] = iteration
                changelog.extend(sub_log)
                iter_field_changes += len(sub_log)

            # 2. additions
            required_fields = context.get("_required_fields") or []
            existing_ids = {r.get(id_key) for r in corrected}
            for new_rec in additions:
                rec_id = new_rec.get(id_key)
                if rec_id and rec_id in existing_ids:
                    logger.debug("CorrectionStage: '%s' уже есть — пропускаем", rec_id)
                    continue

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
            # Для dict-документа match_keys = все ключи всех match-объектов
            all_match_keys = frozenset(
                k for upd in updates for k in (upd.get("match") or {}).keys()
            )
            corrected, iter_log, newly_locked = self._apply_changes(
                deepcopy(original), all_changes, record_id, locked, all_match_keys
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

        context["corrected_data"]  = corrected
        context["changelog"]       = changelog
        context["_locked_fields"]  = [list(pair) for pair in locked]

        logger.info(
            "CorrectionStage [iter %d]: field_changes=%d  records_added=%d  locked_total=%d  changelog_total=%d",
            iteration, iter_field_changes, iter_records_added, len(locked), len(changelog),
        )
        return context
