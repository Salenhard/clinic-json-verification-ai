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
Задача: проверить, насколько JSON-документ соответствует клиническим рекомендациям и выдать ТОЛЬКО конкретные действия по исправлению.

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

Правила вывода:
- НЕ ссылаться на текст рекомендаций
- НЕ описывать проблему в общих словах
- Говорить ТОЛЬКО: что добавить → куда → какое значение
- Говорить ТОЛЬКО: что исправить → где → старое → новое

Верни ТОЛЬКО валидный JSON:
{{
  "completeness_score": 0.0,
  "issues": [
    {{
      "severity": "critical|warning|info",
      "field": "имя поля или null",
      "description": "точное описание проблемы с указанием конкретного поля/значения",
      "suggestion": "точное описание того что нужно сделать"
    }}
  ],
  "missing_fields": ["поле1", "поле2"],
  "suggestions": {{"поле": "рекомендуемое значение согласно рекомендациям"}},
  "overall_comment": "..."
}}
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
        all_issues = []
        all_missing: set[str] = set()
        all_suggestions: dict = {}
        scores = []

        for r in results:
            all_issues.extend(r.get("issues", []))
            all_missing.update(r.get("missing_fields", []))
            all_suggestions.update(r.get("suggestions", {}))
            score = r.get("completeness_score")
            if score is not None:
                scores.append(float(score))

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
            "Analysis: score=%.2f  issues=%d  missing_fields=%d",
            analysis.get("completeness_score", 0),
            len(analysis.get("issues", [])),
            len(analysis.get("missing_fields", [])),
        )
        return context
