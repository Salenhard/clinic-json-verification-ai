"""LLM provider adapters — единый интерфейс для разных моделей."""
import os
from abc import ABC, abstractmethod
from typing import Optional


class LLMAdapter(ABC):
    """Абстрактный адаптер. Реализуй один метод — и всё работает."""

    @abstractmethod
    def complete(self, prompt: str, system: Optional[str] = None) -> str:
        ...

    @property
    @abstractmethod
    def model_name(self) -> str:
        ...


# ── Gemini ────────────────────────────────────────────────────────────────────

class GeminiAdapter(LLMAdapter):
    def __init__(self, client, model: str = "gemini-2.0-flash",
                 max_output_tokens: int = 65536):
        self._client = client
        self._model = model
        self._max_tokens = max_output_tokens

    @property
    def model_name(self) -> str:
        return self._model

    def complete(self, prompt: str, system: Optional[str] = None) -> str:
        from google.genai import types as genai_types
        config = genai_types.GenerateContentConfig(
            temperature=0.1,
            max_output_tokens=self._max_tokens,
            system_instruction=system,
        )
        response = self._client.models.generate_content(
            model=self._model, contents=prompt, config=config,
        )
        return response.text


# ── OpenAI-совместимые (GPT, Grok, DeepSeek) ─────────────────────────────────

class OpenAICompatibleAdapter(LLMAdapter):
    """
    Работает с любым провайдером, у которого есть OpenAI-совместимый API.
    Grok:     base_url="https://api.x.ai/v1",     model="grok-3"
    DeepSeek: base_url="https://api.deepseek.com/v1", model="deepseek-chat"
    GPT:      base_url=None (дефолт openai),       model="gpt-4o"
    """
    def __init__(self, model: str, api_key: str,
                 base_url: Optional[str] = None,
                 max_tokens: int = 8192):
        try:
            from openai import OpenAI
        except ImportError:
            raise ImportError("pip install openai")
        self._client = OpenAI(api_key=api_key, base_url=base_url)
        self._model = model
        self._max_tokens = max_tokens

    @property
    def model_name(self) -> str:
        return self._model

    def complete(self, prompt: str, system: Optional[str] = None) -> str:
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})
        resp = self._client.chat.completions.create(
            model=self._model,
            messages=messages,
            temperature=0.1,
            max_tokens=self._max_tokens,
        )
        return resp.choices[0].message.content


# ── Anthropic Claude ──────────────────────────────────────────────────────────

class ClaudeAdapter(LLMAdapter):
    def __init__(self, model: str = "claude-opus-4-5",
                 api_key: Optional[str] = None,
                 max_tokens: int = 8192):
        try:
            import anthropic
        except ImportError:
            raise ImportError("pip install anthropic")
        self._client = anthropic.Anthropic(api_key=api_key or os.environ["ANTHROPIC_API_KEY"])
        self._model = model
        self._max_tokens = max_tokens

    @property
    def model_name(self) -> str:
        return self._model

    def complete(self, prompt: str, system: Optional[str] = None) -> str:
        kwargs = dict(
            model=self._model,
            max_tokens=self._max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        if system:
            kwargs["system"] = system
        resp = self._client.messages.create(**kwargs)
        return resp.content[0].text


# ── Фабрика ───────────────────────────────────────────────────────────────────

class LLMAdapterFactory:
    """
    Создаёт адаптер по имени провайдера.

    Пример:
        adapter = LLMAdapterFactory.create("grok",
            model="grok-3", api_key="xai-...")
    """

    _registry = {
        "gemini": GeminiAdapter,
        "claude": ClaudeAdapter,
        "openai": OpenAICompatibleAdapter,
        "gpt":    OpenAICompatibleAdapter,
        "grok": lambda **kw: OpenAICompatibleAdapter(
            base_url="https://api.x.ai/v1", **kw),
        "deepseek": lambda **kw: OpenAICompatibleAdapter(
            base_url="https://api.deepseek.com/v1", **kw),
    }

    @classmethod
    def create(cls, provider: str, **kwargs) -> LLMAdapter:
        provider = provider.lower()
        builder = cls._registry.get(provider)
        if builder is None:
            raise ValueError(f"Unknown provider '{provider}'. "
                             f"Available: {list(cls._registry)}")
        return builder(**kwargs)

    @classmethod
    def register(cls, name: str, builder) -> None:
        """Зарегистрируй свой провайдер без правки этого файла."""
        cls._registry[name] = builder