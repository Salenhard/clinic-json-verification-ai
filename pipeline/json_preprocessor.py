import io
import logging
from typing import Dict, Any, List

from .base import BasePipelineStage, PipelineError
from .chunker import TextChunker, Chunk

logger = logging.getLogger(__name__)

PDF_MAX_CHARS = 120_000


class JsonPreprocessor(BasePipelineStage):
    def __init__(self, client, model: str = "gemini-2.0-flash", requests_per_minute: int = 15):
        super().__init__(client, model, requests_per_minute)
        self.chunker = TextChunker(max_chars=12_000, overlap_chars=400, min_chunk_chars=300)

    def _extract_pdf_text(self, pdf_bytes) -> str:
        import pdfplumber

        pdf_file = io.BytesIO(pdf_bytes)
        pdf_file.seek(0)

        text_parts = []

        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                t = page.extract_text() or ""
                if t.strip():
                    text_parts.append(t)

        text = "\n".join(text_parts)

        if len(text) > PDF_MAX_CHARS:
            text = text[:PDF_MAX_CHARS]

        return text

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        data = context.get("input_data")
        if not isinstance(data, (dict, list)):
            raise PipelineError("input_data должен быть JSON")

        context["original_data"] = data

        text = context.get("recommendations", "") or ""

        pdf_bytes = context.get("recommendations_bytes")
        filename = context.get("recommendations_filename") or ""

        if pdf_bytes:
            if filename and not filename.lower().endswith(".pdf"):
                raise PipelineError("Только PDF поддерживается")

            text = self._extract_pdf_text(pdf_bytes)

        if not text.strip():
            raise PipelineError("Нет рекомендаций")

        chunks: List[Chunk] = self.chunker.split(text)

        context["recommendation_chunks"] = chunks
        context["recommendations_full_text"] = text

        return context