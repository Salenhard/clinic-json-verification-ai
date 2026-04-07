import json
import logging
import re
import time
from abc import ABC, abstractmethod
from typing import List, Optional

from google.genai import types as genai_types

from .rate_limiter import get_limiter
from .chunker import Chunk

logger = logging.getLogger(__name__)


class PipelineError(Exception):
    pass


class BasePipelineStage(ABC):
    stage_name: str = "base"
    MAX_RETRIES = 3
    RETRY_DELAY = 2
    RATE_LIMIT_BACKOFF = 65
    MAX_OUTPUT_TOKENS = 65536

    _SYSTEM_INSTRUCTION = (
        "Ты анализатор текста по клиническим рекомендациям и медицинским стандартам. "
        "Отвечай только валидным JSON без лишних пояснений и markdown-разметки. "
        "Используй только сведения из предоставленного текста — не придумывай данные."
    )
    def __init__(
        self,
        adapter: "LLMAdapter",
        requests_per_minute: int = 15,
    ):
        self.adapter = adapter
        self.model = adapter.model_name  # для логов совместимость
        self.tokens_used = 0
        self._limiter = get_limiter(requests_per_minute)

    def _call_llm(self, prompt: str, system: Optional[str] = None) -> str:
        self._limiter.acquire()
        sys_instr = system if system else self._SYSTEM_INSTRUCTION
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                return self.adapter.complete(prompt, system=sys_instr)
            except Exception as exc:
                s = str(exc).lower()
                if "429" in s or "quota" in s or "rate" in s:
                    logger.warning("%s: rate limit — backoff %ds",
                                   self.stage_name, self.RATE_LIMIT_BACKOFF)
                    time.sleep(self.RATE_LIMIT_BACKOFF)
                elif attempt == self.MAX_RETRIES:
                    raise
        raise PipelineError("unreachable")
    # ── JSON helpers ──────────────────────────────────────────────────────────

    def _clean_json(self, text: str) -> str:
        text = re.sub(r"```(?:json)?\s*", "", text)
        text = re.sub(r"```\s*", "", text)
        text = text.strip()
        start_brace = text.find("{")
        start_arr = text.find("[")
        if start_brace == -1 and start_arr == -1:
            return text
        if start_brace == -1:
            start = start_arr
        elif start_arr == -1:
            start = start_brace
        else:
            start = min(start_brace, start_arr)
        end = text.rfind("}")
        end_arr = text.rfind("]")
        if end == -1 and end_arr == -1:
            return text[start:]
        return text[start: max(end, end_arr) + 1]

    # ── JSON repair ───────────────────────────────────────────────────────────

    def _repair_json(self, broken_text: str) -> dict:
        logger.warning("%s: attempting JSON repair", self.stage_name)
        fixed = self._call_llm(
            "The following text should be valid JSON but has syntax errors. "
            "Fix it and return ONLY valid JSON, no explanations, no markdown:\n\n"
            + broken_text[:4000]
        )
        return json.loads(self._clean_json(fixed))

    # ── Retry wrapper ─────────────────────────────────────────────────────────

    def _execute_with_retry(self, prompt: str) -> dict:
        last_error = None
        raw = ""
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                raw = self._call_llm(prompt)
                return json.loads(self._clean_json(raw))
            except json.JSONDecodeError as e:
                logger.warning("%s attempt %d/%d: JSON error — %s", self.stage_name, attempt, self.MAX_RETRIES, e)
                last_error = e
                if attempt == self.MAX_RETRIES:
                    try:
                        return self._repair_json(raw)
                    except Exception as re_err:
                        raise PipelineError(f"{self.stage_name}: JSON repair failed: {re_err}") from re_err
            except PipelineError as e:
                logger.warning("%s attempt %d/%d: %s", self.stage_name, attempt, self.MAX_RETRIES, e)
                last_error = e
            except Exception as e:
                logger.error("%s attempt %d/%d unexpected: %s", self.stage_name, attempt, self.MAX_RETRIES, e)
                last_error = e
            if attempt < self.MAX_RETRIES:
                time.sleep(self.RETRY_DELAY * attempt)

        raise PipelineError(f"{self.stage_name} failed after {self.MAX_RETRIES} attempts. Last: {last_error}")

    # ── Chunked execution ─────────────────────────────────────────────────────

    CHUNK_WORKERS = 3

    def _execute_over_chunks(
        self,
        chunks: List[Chunk],
        build_prompt_fn,
        merge_fn,
    ) -> dict:
        import concurrent.futures

        if len(chunks) == 1:
            return self._execute_with_retry(build_prompt_fn(chunks[0].text, 0, 1))

        def _process_chunk(chunk: Chunk) -> Optional[dict]:
            logger.info("%s: chunk %d/%d (%d chars)", self.stage_name, chunk.index + 1, len(chunks), chunk.char_count)
            try:
                return self._execute_with_retry(build_prompt_fn(chunk.text, chunk.index, len(chunks)))
            except PipelineError as e:
                logger.error("%s chunk %d failed: %s", self.stage_name, chunk.index, e)
                return None

        workers = min(self.CHUNK_WORKERS, len(chunks))
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_process_chunk, chunk): chunk for chunk in chunks}
            ordered_results: List[Optional[dict]] = [None] * len(chunks)
            for fut in concurrent.futures.as_completed(futures):
                chunk = futures[fut]
                ordered_results[chunk.index] = fut.result()

        results = [r for r in ordered_results if r is not None]
        if not results:
            raise PipelineError(f"{self.stage_name}: all chunks failed")

        merged = merge_fn(results)
        logger.info("%s: merged %d/%d chunk results", self.stage_name, len(results), len(chunks))
        return merged

    @abstractmethod
    def run(self, context: dict) -> dict:
        pass
