"""Stage 2 — Analyse JSON against clinical guidelines.

Processes guidelines in chunks (for large PDFs) and merges results.

На выходе добавляет `supplement_json` — патч с полями для дополнения/исправления
исходного JSON. Используется `CorrectionStage` для прямого merge без LLM-вызова.
"""
import json
import logging
from typing import Dict, Any

from .base import BasePipelineStage, PipelineError

logger = logging.getLogger(__name__)


class AnalysisStage(BasePipelineStage):
    stage_name = "stage2_analysis"
    MAX_OUTPUT_TOKENS = 32768

    _PROMPT_TEMPLATE = """\
Задача: проверить, насколько JSON-документ соответствует клиническим рекомендациям,
и сформировать патч для его дополнения.

JSON-ДОКУМЕНТ:
{json_data}

ФРАГМЕНТ КЛИНИЧЕСКИХ РЕКОМЕНДАЦИЙ (часть {chunk_index} из {total_chunks}):
{chunk_text}

Проверь:
1. Полноту (все ли поля, которые должны быть по рекомендациям, присутствуют)
2. Корректность значений (соответствуют ли они рекомендациям)
3. Отсутствующие критически важные данные
4. Противоречия с рекомендациями
5. Покрывает ли документ все клинические рекомендации

Верни ТОЛЬКО валидный JSON (без markdown):
{{
  "completeness_score": 0.0,
  "supplement_json": {{
    "ПРАВИЛА": Структура = структура исходного JSON. Значения строго из рекомендаций. Недостающие сущности или те которые нужно заменить описывай их полностью"
  }},
}}
"""

    def _build_prompt(self, chunk_text: str, chunk_index: int, total_chunks: int, json_data: str) -> str:
        return self._PROMPT_TEMPLATE.format(
            json_data=json_data,
            chunk_text=chunk_text,
            chunk_index=chunk_index,
            total_chunks=total_chunks
        )

    @staticmethod
    def _deep_merge(base: dict, override: dict) -> dict:
        """Deep merge override into base. Override wins on scalar conflicts."""
        result = dict(base)
        for k, v in override.items():
            if k in result and isinstance(result[k], dict) and isinstance(v, dict):
                result[k] = AnalysisStage._deep_merge(result[k], v)
            elif isinstance(v, list) and k in result and isinstance(result[k], list):
                # Объединяем списки без дублирования строк
                existing = list(result[k])
                for item in v:
                    if item not in existing:
                        existing.append(item)
                result[k] = existing
            else:
                result[k] = v
        return result

    @staticmethod
    def _merge_results(results: list) -> dict:
        all_issues = []
        all_missing: set[str] = set()
        all_suggestions: dict = {}
        all_supplement: dict = {}
        scores = []

        for r in results:
            all_issues.extend(r.get("issues", []))
            all_missing.update(r.get("missing_fields", []))
            all_suggestions.update(r.get("suggestions", {}))
            score = r.get("completeness_score")
            if score is not None:
                scores.append(float(score))

            supplement = r.get("supplement_json")
            if isinstance(supplement, dict):
                all_supplement = AnalysisStage._deep_merge(all_supplement, supplement)

        seen: set[tuple] = set()
        unique_issues = []
        for iss in all_issues:
            key = (iss.get("field"), iss.get("description", "")[:80])
            if key not in seen:
                seen.add(key)
                unique_issues.append(iss)

        return {
            "completeness_score": round(sum(scores) / len(scores), 3) if scores else 0.0,
            "issues": unique_issues,
            "missing_fields": sorted(all_missing),
            "suggestions": all_suggestions,
            "supplement_json": all_supplement,
            "overall_comment": f"Проанализировано {len(results)} фрагментов рекомендаций.",
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
            "Analysis: score=%.2f  issues=%d  missing_fields=%d  supplement_keys=%d",
            analysis.get("completeness_score", 0),
            len(analysis.get("issues", [])),
            len(analysis.get("missing_fields", [])),
            len(analysis.get("supplement_json", {})),
        )
        return context