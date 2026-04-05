from .base import BasePipelineStage, PipelineError
import json
from typing import Dict, Any

class AnalysisStage(BasePipelineStage):
    stage_name = "stage2_analysis"
    MAX_OUTPUT_TOKENS = 32768

    _PROMPT = """\
Задача: проверить, насколько JSON соответствует клиническим рекомендациям.

JSON:
{json_data}

КЛИНИЧЕСКИЕ РЕКОМЕНДАЦИИ:
{recommendations}

Проверь:
1. Полноту (есть ли все важные поля, которые должны быть по рекомендациям)
2. Соответствие значений рекомендациям
3. Отсутствующие критически важные данные
4. Возможные противоречия

Верни ТОЛЬКО валидный JSON (без markdown):
{{
  "completeness_score": 0.0,
  "issues": [
    {{
      "severity": "critical|warning|info",
      "field": "имя_поля_или_null",
      "description": "точное описание проблемы",
      "suggestion": "что нужно сделать"
    }}
  ],
  "missing_fields": ["поле1", "поле2"],
  "suggestions": {{"поле": "рекомендуемое значение или пояснение"}},
  "overall_comment": "..."
}}
"""

        def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        self.update_progress(context, 30, "LLM-анализ рекомендаций по чанкам...")

        json_data = json.dumps(context["original_data"], ensure_ascii=False, indent=2)
        chunks = context["recommendation_chunks"]

        def build_prompt(chunk_text: str, chunk_index: int, total: int) -> str:
            return self._PROMPT_TEMPLATE.format(
                chunk_text=chunk_text,
                json_data=json_data
            )

        def merge_fn(results: list) -> dict:
            all_issues = []
            all_suggestions = {}
            for r in results:
                all_issues.extend(r.get("issues", []))
                all_suggestions.update(r.get("suggestions", {}))
            return {
                "issues": all_issues,
                "missing_fields": list(set().union(*(r.get("missing_fields", []) for r in results))),
                "suggestions": all_suggestions,
                "overall_comment": f"Проанализировано {len(results)} чанков"
            }

        if len(chunks) > 1:
            analysis = self._execute_over_chunks(
                chunks=chunks,
                build_prompt_fn=build_prompt,
                merge_fn=merge_fn
            )
        else:
            prompt = build_prompt(chunks[0].text, 0, 1)
            analysis = self._execute_with_retry(prompt)

        context["analysis"] = analysis
        self.update_progress(context, 50, f"Анализ завершён ({len(chunks)} чанков)")
        return context