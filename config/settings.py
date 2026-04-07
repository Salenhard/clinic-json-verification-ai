"""Application configuration — loaded once at startup."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    gemini_api_key: str = field(default_factory=lambda: os.getenv("GEMINI_API_KEY", ""))
    gemini_model: str = field(default_factory=lambda: os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite-preview"))
    requests_per_minute: int = field(default_factory=lambda: int(os.getenv("RPM", "15")))
    db_path: str = field(default_factory=lambda: os.getenv("DB_PATH", "tasks.db"))
    max_iterations: int = field(default_factory=lambda: int(os.getenv("MAX_ITERATIONS", "5")))
    target_score: float = field(default_factory=lambda: float(os.getenv("TARGET_SCORE", "1.0")))
    max_content_length_mb: int = field(default_factory=lambda: int(os.getenv("MAX_CONTENT_MB", "50")))

    def validate(self) -> None:
        if not self.gemini_api_key:
            raise RuntimeError("GEMINI_API_KEY is not set in environment or .env file")

    @property
    def max_content_length_bytes(self) -> int:
        return self.max_content_length_mb * 1024 * 1024


# Singleton — import this everywhere
settings = Settings()
