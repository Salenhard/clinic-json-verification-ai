import re
import logging
from dataclasses import dataclass, field
from typing import List

logger = logging.getLogger(__name__)

@dataclass
class Chunk:
    index: int
    text: str
    start_char: int
    end_char: int

    @property
    def char_count(self) -> int:
        return len(self.text)

    def __repr__(self) -> str:
        return (
            f"Chunk(index={self.index}, chars={self.char_count:,}, "
            f"orig={self.start_char}:{self.end_char})"
        )

_BOUNDARIES = [
    # Numbered section like "1.", "2.3", "4.1.2" at the start of a line
    re.compile(r"(?m)^(?=\d+(?:\.\d+)*\.?\s+\S)"),
    # Line that is ALL CAPS (common heading style in Russian clinical docs)
    re.compile(r"\n(?=[А-ЯЁA-Z][А-ЯЁA-Z\s]{3,}\n)"),
    # Double blank line
    re.compile(r"\n{2,}"),
    # Single newline
    re.compile(r"\n"),
    # Sentence end: period/!/ followed by whitespace + capital
    re.compile(r"(?<=[.!?])\s+(?=[А-ЯЁA-Z0-9])"),
]

class TextChunker:
    def __init__(
        self,
        max_chars: int = 12_000,
        overlap_chars: int = 400,
        min_chunk_chars: int = 300,
        overlap_marker: str = "\n[...]\n",
    ):
        self.max_chars = max_chars
        self.overlap_chars = overlap_chars
        self.min_chunk_chars = min_chunk_chars
        self.overlap_marker = overlap_marker

    def split(self, text: str) -> List[Chunk]:
        if not text:
            return []

        boundary_positions = self._find_boundaries(text)
        raw = self._merge_into_raw(text, boundary_positions)
        chunks = self._build_chunks(text, raw)
        chunks = self._merge_tiny(chunks)

        logger.info(
            "Chunker: %d chars → %d chunks "
            "(max=%d, overlap=%d)",
            len(text), len(chunks), self.max_chars, self.overlap_chars,
        )
        for c in chunks:
            logger.debug("  %s", c)

        return chunks

    def _find_boundaries(self, text: str) -> List[int]:
        positions: set[int] = {0, len(text)}
        for pattern in _BOUNDARIES:
            for m in pattern.finditer(text):
                positions.add(m.start())
        return sorted(positions)

    def _merge_into_raw(self, text: str, positions: List[int]) -> List[dict]:
        """
        Walk boundary positions and flush a raw chunk whenever adding the next
        segment would exceed max_chars.  Hard-splits segments that are
        themselves larger than max_chars.
        """
        raw: List[dict] = []
        chunk_start = 0

        for i in range(1, len(positions)):
            seg_end = positions[i]
            current_size = seg_end - chunk_start

            if current_size < self.max_chars:

            prev_end = positions[i - 1]
            if prev_end > chunk_start:
                raw.append({"start": chunk_start, "end": prev_end})
                chunk_start = prev_end

            seg_start = chunk_start
            while seg_end - seg_start > self.max_chars:
                split_at = seg_start + self.max_chars
                raw.append({"start": seg_start, "end": split_at})
                seg_start = split_at
            chunk_start = seg_start

        if chunk_start < len(text):
            raw.append({"start": chunk_start, "end": len(text)})

        return raw

    def _build_chunks(self, text: str, raw: List[dict]) -> List[Chunk]:
        chunks: List[Chunk] = []
        for i, rc in enumerate(raw):
            body = text[rc["start"]:rc["end"]]
            if i > 0 and self.overlap_chars > 0:
                tail = chunks[i - 1].text[-self.overlap_chars:]
                body = tail + self.overlap_marker + body
            chunks.append(Chunk(
                index=i,
                text=body,
                start_char=rc["start"],
                end_char=rc["end"],
            ))
        return chunks

    def _merge_tiny(self, chunks: List[Chunk]) -> List[Chunk]:
        if not chunks:
            return chunks
        result: List[Chunk] = [chunks[0]]
        for c in chunks[1:]:
            orig_size = c.end_char - c.start_char
            if orig_size < self.min_chunk_chars:
                prev = result[-1]
                result[-1] = Chunk(
                    index=prev.index,
                    text=prev.text + c.text,
                    start_char=prev.start_char,
                    end_char=c.end_char,
                )
            else:
                result.append(c)
        # Re-number after merging
        for idx, c in enumerate(result):
            c.index = idx
        return result

def chunk_summary(chunks: List[Chunk]) -> str:
    if not chunks:
        return "0 chunks"
    sizes = [c.char_count for c in chunks]
    return (
        f"{len(chunks)} chunks — "
        f"min {min(sizes):,} / avg {int(sum(sizes)/len(sizes)):,} / max {max(sizes):,} chars"
    )
