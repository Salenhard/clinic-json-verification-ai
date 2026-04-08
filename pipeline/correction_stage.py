"""Stage 4 — Supplement and correct the JSON using the analysis patch.

Основной путь: берёт `analysis.supplement_json` (патч, созданный на этапе анализа)
и делает deep-merge в оригинальный JSON — без дополнительного LLM-вызова.

Правила применения патча:
  • Новые ключи (отсутствующие в оригинале) — добавляются всегда.
  • Существующие null / "" / [] — заполняются значением из патча.
  • Существующие непустые значения — перезаписываются ТОЛЬКО если поле
    помечено как critical в analysis.issues.
  • Структура верхнего уровня оригинала сохраняется (ключи не удаляются).
"""
import logging
from copy import deepcopy
from typing import Any, Dict, List, Set, Tuple

from .base import BasePipelineStage

logger = logging.getLogger(__name__)

_EMPTY = (None, "", [], {})


def _is_empty(v: Any) -> bool:
    return v in _EMPTY or v is None


class CorrectionStage(BasePipelineStage):
    stage_name = "stage4_correction"

    # ── Helpers ──────────────────────────────────────────────────────────────

    @staticmethod
    def _critical_fields(issues: list) -> Set[str]:
        """Возвращает множество полей с severity=critical из анализа."""
        return {
            iss["field"]
            for iss in issues
            if iss.get("severity") == "critical" and iss.get("field")
        }

    def _apply_patch(
        self,
        original: Any,
        patch: Any,
        critical: Set[str],
        path: str = "",
    ) -> Tuple[Any, List[dict]]:
        """
        Рекурсивно применяет patch к original.
        Возвращает (обновлённый объект, список записей changelog).
        """
        changelog: List[dict] = []

        if not isinstance(patch, dict) or not isinstance(original, dict):
            # Для скалярных/списочных узлов — просто заменяем если пусто или critical
            if _is_empty(original) or path in critical:
                if original != patch:
                    changelog.append({
                        "action": "modified" if not _is_empty(original) else "added",
                        "field": path,
                        "old_value": original,
                        "new_value": patch,
                        "reason": "critical issue" if path in critical else "пустое поле заполнено из анализа",
                    })
                return patch, changelog
            return original, changelog

        result = deepcopy(original)

        for key, patch_val in patch.items():
            full_path = f"{path}.{key}" if path else key
            orig_val = result.get(key)

            if key not in result:
                # Новый ключ — добавляем
                result[key] = patch_val
                changelog.append({
                    "action": "added",
                    "field": full_path,
                    "old_value": None,
                    "new_value": patch_val,
                    "reason": "отсутствующее поле добавлено согласно рекомендациям",
                })

            elif isinstance(orig_val, dict) and isinstance(patch_val, dict):
                # Рекурсия для вложенных объектов
                merged, sub_log = self._apply_patch(orig_val, patch_val, critical, full_path)
                result[key] = merged
                changelog.extend(sub_log)

            elif _is_empty(orig_val):
                # Пустое / null — заполняем
                result[key] = patch_val
                changelog.append({
                    "action": "added",
                    "field": full_path,
                    "old_value": orig_val,
                    "new_value": patch_val,
                    "reason": "пустое поле заполнено согласно рекомендациям",
                })

            elif full_path in critical or key in critical:
                # Critical issue — исправляем даже непустое значение
                result[key] = patch_val
                changelog.append({
                    "action": "modified",
                    "field": full_path,
                    "old_value": orig_val,
                    "new_value": patch_val,
                    "reason": "значение исправлено: critical issue согласно рекомендациям",
                })

        return result, changelog

    # ── Stage entry point ────────────────────────────────────────────────────

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        original = context["original_data"]
        analysis = context["analysis"]
        supplement = analysis.get("supplement_json", {})

        if not isinstance(supplement, dict) or not supplement:
            logger.info("CorrectionStage: supplement_json пустой — пропускаем")
            context["corrected_data"] = deepcopy(original)
            context["changelog"] = []
            return context

        critical = self._critical_fields(analysis.get("issues", []))

        corrected, changelog = self._apply_patch(original, supplement, critical)

        # Гарантируем, что ни один ключ верхнего уровня не потерян
        if isinstance(original, dict):
            for key in original:
                if key not in corrected:
                    logger.warning("CorrectionStage: ключ '%s' пропал — восстанавливаем", key)
                    corrected[key] = original[key]

        context["corrected_data"] = corrected
        context["changelog"] = changelog

        logger.info(
            "CorrectionStage: применено %d изменений (%d critical полей)",
            len(changelog),
            len(critical),
        )
        return context
