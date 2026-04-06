"""Stage 1 — Preprocess: extract PDF text, validate inputs, split into chunks."""
import logging
from typing import Dict, Any, List
import io
from .base import BasePipelineStage, PipelineError
from .chunker import TextChunker, Chunk

logger = logging.getLogger(__name__)

PDF_MAX_CHARS = 120_000  # ~30k tokens — enough context for any clinical guideline


class JsonPreprocessor(BasePipelineStage):
    stage_name = "stage1_preprocessor"

    def __init__(self, client, model: str = "gemini-2.0-flash", requests_per_minute: int = 15):
        super().__init__(client, model, requests_per_minute)
        self.chunker = TextChunker(max_chars=12_000, overlap_chars=400, min_chunk_chars=300)

    def _extract_pdf_text(self, pdf_file) -> str:
        try:
            import pdfplumber
        except ImportError as e:
            raise PipelineError("pdfplumber не установлен: pip install pdfplumber") from e

        if not pdf_file:
            return ""
        text_parts: list[str] = []
        try:
            with pdfplumber.open(pdf_file) as pdf:
                logger.info("PDF: %d страниц", len(pdf.pages))
                for i, page in enumerate(pdf.pages, 1):
                    t = page.extract_text() or ""
                    if t.strip():
                        text_parts.append(t)
                    if i % 20 == 0 or i == len(pdf.pages):
                        logger.debug("  Обработано %d/%d страниц", i, len(pdf.pages))
        except PipelineError:
            raise
        except Exception as e:
            raise PipelineError(f"Ошибка чтения PDF: {e}") from e

        full_text = "\n\n".join(text_parts)
        if len(full_text) > PDF_MAX_CHARS:
            logger.warning("PDF текст обрезан: %d → %d символов", len(full_text), PDF_MAX_CHARS)
            full_text = full_text[:PDF_MAX_CHARS]

        logger.info("PDF извлечён: %d символов", len(full_text))
        return full_text

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        # ── Validate input JSON ───────────────────────────────────────────────
        data = context.get("input_data")
        if not isinstance(data, (dict, list)):
            raise PipelineError("input_data должен быть объектом JSON (dict или list)")
        context["original_data"] = data

        # ── Extract guidelines text ───────────────────────────────────────────
        recommendations_text: str = context.get("recommendations", "") or ""
        pdf_bytes = context.get("recommendations_bytes")
        if pdf_bytes:
            pdf_file = io.BytesIO(pdf_bytes)
        if pdf_file:
            fname = getattr(pdf_file, "filename", "") or ""
            if fname.lower().endswith(".pdf"):
                recommendations_text = self._extract_pdf_text(pdf_file)
            else:
                raise PipelineError(f"Неподдерживаемый формат файла: {fname}. Ожидается .pdf")

        if not recommendations_text.strip():
            raise PipelineError(
                "Клинические рекомендации не переданы. "
                "Передайте PDF-файл в поле 'recommendations_file' или текст в 'recommendations'."
            )

        # ── Split into chunks ─────────────────────────────────────────────────
        chunks: List[Chunk] = self.chunker.split(recommendations_text)
        logger.info("Разбито на %d чанков", len(chunks))

        context["recommendation_chunks"] = chunks
        context["recommendations_full_text"] = recommendations_text
        return context
