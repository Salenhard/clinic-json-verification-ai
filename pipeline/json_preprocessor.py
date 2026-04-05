from .base import BasePipelineStage, PipelineError
from .chunker import TextChunker, Chunk
from typing import Dict, Any, List
import pdfplumber
import logging

logger = logging.getLogger(__name__)

class JsonPreprocessor(BasePipelineStage):
    stage_name = "stage1_preprocessor"

    def __init__(self, client, model: str = "gemini-2.0-flash", requests_per_minute: int = 15):
        super().__init__(client, model, requests_per_minute)
        self.chunker = TextChunker(max_chars=12000, overlap_chars=400, min_chunk_chars=300)

    def _extract_pdf_text(self, pdf_file) -> str:
        """Извлечение ПОЛНОГО текста из PDF без обрезания."""
        if not pdf_file:
            return ""
        text_parts = []
        try:
            with pdfplumber.open(pdf_file) as pdf:
                logger.info(f"PDF: {len(pdf.pages)} страниц. Извлечение полного текста...")
                for i, page in enumerate(pdf.pages, 1):
                    page_text = page.extract_text() or ""
                    if page_text.strip():
                        text_parts.append(page_text)
                    if i % 20 == 0 or i == len(pdf.pages):
                        logger.debug(f"  Обработано {i}/{len(pdf.pages)} страниц")
            full_text = "\n\n".join(text_parts)
            logger.info(f"PDF extracted: {len(full_text):,} символов")
            return full_text
        except Exception as e:
            logger.error(f"Ошибка извлечения PDF: {e}")
            raise PipelineError(f"Не удалось извлечь текст из PDF: {e}") from e

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        self.update_progress(context, 10, "Предобработка JSON и рекомендаций...")
        data = context["input_data"]
        if not isinstance(data, dict):
            raise PipelineError("input_data должен быть dict")

        context["original_data"] = data.copy()

        recommendations_text = context.get("recommendations", "")
        pdf_file = context.get("recommendations_file")

        if pdf_file and getattr(pdf_file, 'filename', '').lower().endswith('.pdf'):
            self.update_progress(context, 12, "Извлечение полного текста из PDF...")
            recommendations_text = self._extract_pdf_text(pdf_file)

        if not recommendations_text.strip():
            raise PipelineError("Не переданы клинические рекомендации")

        self.update_progress(context, 15, "Разбиение рекомендаций на чанки...")
        chunks: List[Chunk] = self.chunker.split(recommendations_text)

        context["recommendation_chunks"] = chunks
        context["recommendations_full_text"] = recommendations_text   # полный текст сохраняется

        self.update_progress(context, 20, f"Предобработка завершена: {len(chunks)} чанков")
        return context