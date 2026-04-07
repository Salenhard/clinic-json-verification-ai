"""Stage 2 — Analyse JSON against clinical guidelines.

Improvements over v1:
  - Prompt is schema-aware: describes all fields, valid enum values, nullability rules.
  - LLM analyses per-method instead of per-field — issues now carry 'method' index.
  - Detects missing methods (present in recommendations but absent from document).
  - Catches Cyrillic/Latin mixing in evidence_level (А/В vs A/B).
  - completeness_score is scoped per chunk (fraction of methods in this chunk
    that are fully represented), making the averaged final score meaningful.
  - Merge deduplicates by (method, field) instead of (field, description).
  - overall_comment is synthesised from chunk comments, not a stub.
"""
import json
import logging
from typing import Dict, Any

from .base import BasePipelineStage, PipelineError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema constants — single source of truth for prompt and merge logic
# ---------------------------------------------------------------------------

_SCHEMA_DESCRIPTION = """\
Каждый объект в списке описывает один клинический метод и имеет поля:
  • method             — название метода (строка, обязательно)
  • method_type        — тип: diagnostics | drug | procedure | surgery |
                         treatment | rehabilitation | monitoring | immobilization
  • diagnosis          — диагноз (строка, обязательно)
  • patient_group      — группа пациентов (строка, обязательно)
  • goal               — цель метода (строка, обязательно)
  • conditions         — показания (список строк; может быть пустым, если показаний нет)
  • contraindications  — противопоказания (список строк; может быть пустым)
  • timing             — сроки/время выполнения (строка или null)
  • dosage             — дозировка/схема (строка или null;
                         КРИТИЧНО для method_type=drug или treatment)
  • recommendation_type — recommended | conditional | may_be_considered | not_recommended
  • evidence_level     — уровень доказательности: ТОЛЬКО латинские буквы A | B | C | D
                         (кириллические А, В, С, D — ошибка кодировки, нужно исправить)
  • evidence_grade     — степень достоверности: 1 | 2 | 3 | 4 | 5
  • source_quote       — дословная цитата из рекомендаций (строка)
  • source_section     — раздел источника (строка или null)
  • source_number      — номер пункта (строка или null)
  • source_page        — страница (число или null)
"""

_PROMPT_TEMPLATE = """\
Ты — валидатор клинических данных. Твоя задача — проверить список JSON-объектов \
на соответствие фрагменту клинических рекомендаций.

=== СХЕМА ДОКУМЕНТА ===
{schema}

=== ФРАГМЕНТ КЛИНИЧЕСКИХ РЕКОМЕНДАЦИЙ (часть {chunk_index} из {total_chunks}) ===
{chunk_text}

=== ДОКУМЕНТ (список клинических методов) ===
{json_data}

=== ЗАДАЧИ ===
1. ОТСУТСТВУЮЩИЕ МЕТОДЫ: найди методы/вмешательства из фрагмента рекомендаций,
   которых НЕТ в документе совсем (нет ни одного объекта с соответствующим method).

2. ПРОБЛЕМЫ В СУЩЕСТВУЮЩИХ ОБЪЕКТАХ — для каждого метода из документа,
   связанного с данным фрагментом рекомендаций, проверь:
   a. dosage — заполнен ли, если в рекомендациях указана конкретная схема/доза?
      (особенно важно для method_type: drug, treatment)
   b. timing — заполнен ли, если в рекомендациях указаны сроки?
   c. conditions / contraindications — соответствуют ли они тексту рекомендаций?
   d. evidence_level — верно ли указан согласно рекомендациям?
      ВНИМАНИЕ: проверь кодировку — допустимы только латинские A, B, C, D.
      Кириллические А, В, С — это ошибка, severity=critical.
   e. evidence_grade — верно ли указан (1–5)?
   f. recommendation_type — соответствует ли формулировке в рекомендациях?

3. ОЦЕНКА completeness_score (0.0–1.0):
   Какая доля методов из ДАННОГО ФРАГМЕНТА рекомендаций полностью и корректно
   представлена в документе? (1.0 = все методы фрагмента присутствуют и заполнены)

Верни ТОЛЬКО валидный JSON без markdown и пояснений:
{{
  "completeness_score": 0.0,
  "issues": [
    {{
      "severity": "critical|warning|info",
      "method": "значение поля method объекта или null",
      "field": "имя проблемного поля или null",
      "description": "конкретное описание проблемы",
      "suggestion": "что именно нужно исправить или добавить"
    }}
  ],
  "missing_methods": ["название метода из рекомендаций 1", "..."],
  "suggestions": {{
    "название_метода.имя_поля": "рекомендуемое значение из текста рекомендаций"
  }},
  "overall_comment": "краткий вывод по данному фрагменту"
}}
"""


class AnalysisStage(BasePipelineStage):
    stage_name = "stage2_analysis"
    MAX_OUTPUT_TOKENS = 32768

    def _build_prompt(
        self,
        chunk_text: str,
        chunk_index: int,
        total_chunks: int,
        json_data: str,
    ) -> str:
        return _PROMPT_TEMPLATE.format(
            schema=_SCHEMA_DESCRIPTION,
            json_data=json_data,
            chunk_text=chunk_text,
            chunk_index=chunk_index + 1,
            total_chunks=total_chunks,
        )

    @staticmethod
    def _merge_results(results: list) -> dict:
        all_issues = []
        all_missing_methods: set[str] = set()
        all_suggestions: dict = {}
        scores = []
        chunk_comments = []

        for r in results:
            all_issues.extend(r.get("issues", []))
            all_missing_methods.update(r.get("missing_methods", []))
            all_suggestions.update(r.get("suggestions", {}))

            score = r.get("completeness_score")
            if score is not None:
                scores.append(float(score))

            comment = r.get("overall_comment", "").strip()
            if comment:
                chunk_comments.append(comment)

        # Deduplicate issues by (method, field, description prefix)
        # v1 deduped only by (field, description) — missed cross-chunk duplicates
        # for the same method.
        seen: set[tuple] = set()
        unique_issues = []
        for iss in all_issues:
            key = (
                iss.get("method"),
                iss.get("field"),
                iss.get("description", "")[:80],
            )
            if key not in seen:
                seen.add(key)
                unique_issues.append(iss)

        # Sort: critical first, then warning, then info
        severity_order = {"critical": 0, "warning": 1, "info": 2}
        unique_issues.sort(
            key=lambda x: severity_order.get(x.get("severity", "info"), 2)
        )

        return {
            "completeness_score": round(sum(scores) / len(scores), 3) if scores else 0.0,
            "issues": unique_issues,
            # Keep 'missing_fields' key for backward compatibility with
            # CorrectionStage and FinalizationStage, but fill it from missing_methods.
            "missing_fields": sorted(all_missing_methods),
            "missing_methods": sorted(all_missing_methods),
            "suggestions": all_suggestions,
            "overall_comment": " | ".join(chunk_comments) if chunk_comments
                               else f"Проанализировано {len(results)} фрагментов.",
        }

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        json_data = json.dumps(context["original_data"], ensure_ascii=False, indent=2)
        chunks = context["recommendation_chunks"]

        analysis = self._execute_over_chunks(
            chunks=chunks,
            build_prompt_fn=lambda text, idx, total: self._build_prompt(
                text, idx, total, json_data
            ),
            merge_fn=self._merge_results,
        )

        context["analysis"] = analysis
        logger.info(
            "Analysis: score=%.2f  issues=%d  missing_methods=%d",
            analysis.get("completeness_score", 0),
            len(analysis.get("issues", [])),
            len(analysis.get("missing_methods", [])),
        )
        return context