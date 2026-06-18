"""LLM and embedding providers with offline-safe defaults and import guards."""

from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Protocol, runtime_checkable

from services.ai.llm.guardrails import apply_guardrails
from services.ai.llm.models import (
    ContextDocument,
    EmbeddingRequest,
    EmbeddingResult,
    GenerationRequest,
    GenerationResult,
)

__all__ = [
    "EmbeddingProvider",
    "LLMProvider",
    "MockLLMProvider",
    "AzureOpenAIProvider",
    "AnthropicProvider",
]

POSITIVE_HINTS = {
    "surge",
    "rally",
    "gain",
    "growth",
    "upbeat",
    "strong",
    "bullish",
    "beat",
}
NEGATIVE_HINTS = {
    "drop",
    "selloff",
    "loss",
    "weak",
    "bearish",
    "risk",
    "lawsuit",
    "hack",
    "decline",
}


@runtime_checkable
class LLMProvider(Protocol):
    """Protocol for providers that generate structured market insight text."""

    async def generate(self, request: GenerationRequest) -> GenerationResult:
        ...


@runtime_checkable
class EmbeddingProvider(Protocol):
    """Protocol for providers that embed text into float vectors."""

    async def embed(self, request: EmbeddingRequest) -> EmbeddingResult:
        ...


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _normalize_label(score: float) -> str:
    if score >= 0.2:
        return "positive"
    if score <= -0.2:
        return "negative"
    return "neutral"


def _stable_digest(parts: tuple[str, ...]) -> bytes:
    joined = "\n".join(parts).encode("utf-8")
    return hashlib.sha256(joined).digest()


def _stable_embedding(text: str, dimensions: int) -> tuple[float, ...]:
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    values: list[float] = []
    index = 0
    while len(values) < dimensions:
        block = hashlib.sha256(digest + bytes([index])).digest()
        for offset in range(0, len(block), 2):
            pair = block[offset : offset + 2]
            number = int.from_bytes(pair, "big")
            values.append((number / 65535.0) * 2.0 - 1.0)
            if len(values) == dimensions:
                break
        index += 1

    norm = math.sqrt(sum(value * value for value in values)) or 1.0
    return tuple(round(value / norm, 6) for value in values)


def _context_blob(context: tuple[ContextDocument, ...]) -> str:
    parts = []
    for document in context:
        parts.append(
            json.dumps(
                {
                    "doc_id": document.doc_id,
                    "title": document.title,
                    "url": document.url,
                    "text": document.text,
                },
                sort_keys=True,
            )
        )
    return "\n".join(parts)


def _message_payload(request: GenerationRequest) -> list[dict[str, str]]:
    messages: list[dict[str, str]] = []
    if request.system_prompt:
        messages.append({"role": "system", "content": request.system_prompt})
    if request.messages:
        for message in request.messages:
            messages.append({"role": message.role, "content": message.content})
    else:
        content = request.prompt
        if request.context:
            context_lines = []
            for document in request.context:
                heading = document.title or document.doc_id
                context_lines.append(f"[{document.citation}] {heading}: {document.text}")
            content = f"{request.prompt}\n\nContext:\n" + "\n".join(context_lines)
        messages.append({"role": "user", "content": content})
    return messages


def _anthropic_message_payload(request: GenerationRequest) -> list[dict[str, str]]:
    if request.messages:
        return [
            {"role": message.role, "content": message.content}
            for message in request.messages
            if message.role in {"user", "assistant"}
        ]

    user_messages = [message for message in _message_payload(request) if message["role"] == "user"]
    return user_messages or [{"role": "user", "content": request.prompt}]


def _anthropic_system_prompt(request: GenerationRequest) -> str:
    parts: list[str] = []
    if request.system_prompt:
        parts.append(request.system_prompt)
    parts.extend(
        message.content for message in request.messages if message.role == "system"
    )
    return "\n\n".join(part for part in parts if part)


def _fake_sentiment(text: str) -> float:
    lowered = text.lower()
    positive_hits = sum(word in lowered for word in POSITIVE_HINTS)
    negative_hits = sum(word in lowered for word in NEGATIVE_HINTS)
    score = (positive_hits - negative_hits) / max(1, positive_hits + negative_hits, 3)
    return round(_clamp(score, -1.0, 1.0), 3)


def _first_sentence(text: str) -> str:
    normalized = " ".join(text.split())
    if not normalized:
        return ""
    for separator in (". ", "! ", "? "):
        if separator in normalized:
            return normalized.split(separator, 1)[0].strip(" .!?")
    return normalized[:180].strip()


def _extract_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts = []
        for item in value:
            if isinstance(item, dict):
                part_text = item.get("text")
                if part_text:
                    parts.append(str(part_text))
            else:
                part_text = getattr(item, "text", None)
                if part_text:
                    parts.append(str(part_text))
        return " ".join(parts)
    return str(value or "")


def _get_attr(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _parse_json_result(raw_text: str) -> dict[str, Any]:
    start = raw_text.find("{")
    end = raw_text.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise ValueError("provider response did not contain JSON")
    return json.loads(raw_text[start : end + 1])


def _structured_from_payload(
    *,
    provider: str,
    model: str,
    payload: dict[str, Any],
    raw_text: str,
) -> GenerationResult:
    score = float(payload["sentiment_score"])
    sentiment_label = payload.get("sentiment_label") or _normalize_label(score)
    citations = tuple(str(citation) for citation in payload.get("citations", ()))
    return GenerationResult(
        summary=str(payload.get("summary", "")).strip(),
        explanation=str(payload.get("explanation", "")).strip(),
        sentiment_score=round(_clamp(score, -1.0, 1.0), 3),
        sentiment_label=sentiment_label,
        citations=citations,
        confidence=round(_clamp(float(payload.get("confidence", 0.0)), 0.0, 1.0), 3),
        grounded=bool(citations),
        provider=provider,
        model=model,
        raw_text=raw_text,
    )


class MockLLMProvider(LLMProvider, EmbeddingProvider):
    """Deterministic offline LLM and embedding provider for tests and local use."""

    def __init__(self, *, model: str = "mock-llm-v1") -> None:
        self.model = model

    async def generate(self, request: GenerationRequest) -> GenerationResult:
        source_parts = (
            request.prompt,
            request.system_prompt or "",
            _context_blob(request.context),
        )
        combined = " ".join(part for part in source_parts if part)
        score = _fake_sentiment(combined)
        sentiment_label = _normalize_label(score)
        top_doc = request.context[0] if request.context else None
        prompt_summary = _first_sentence(request.prompt)
        context_summary = _first_sentence(top_doc.text) if top_doc else prompt_summary
        summary = (
            f"{sentiment_label.title()} outlook for "
            f"{request.metadata.get('symbol', 'the asset')}: {context_summary or prompt_summary}."
        ).strip()
        explanation_parts = []
        if top_doc:
            citation = top_doc.citation
            explanation_parts.append(
                "Grounded in "
                f"{citation}, the key driver is "
                f"{context_summary or 'the retrieved context'}."
            )
        if prompt_summary:
            explanation_parts.append(f"Prompt focus: {prompt_summary}.")
        explanation_parts.append(f"Overall sentiment is {sentiment_label} ({score:+.2f}).")
        explanation = " ".join(explanation_parts)

        digest = _stable_digest(source_parts)
        confidence = 0.55 + (digest[0] / 255.0) * 0.35
        if not request.context:
            confidence = min(confidence, 0.42)

        result = GenerationResult(
            summary=summary,
            explanation=explanation,
            sentiment_score=score,
            sentiment_label=sentiment_label,
            citations=((top_doc.citation,) if top_doc else ()),
            confidence=round(confidence, 3),
            grounded=bool(top_doc),
            provider="mock",
            model=self.model,
            raw_text=json.dumps(
                {
                    "summary": summary,
                    "explanation": explanation,
                    "sentiment_score": score,
                    "sentiment_label": sentiment_label,
                    "citations": [top_doc.citation] if top_doc else [],
                    "confidence": round(confidence, 3),
                },
                sort_keys=True,
            ),
        )
        guarded, _ = apply_guardrails(request, result)
        return guarded

    async def embed(self, request: EmbeddingRequest) -> EmbeddingResult:
        vectors = tuple(_stable_embedding(text, request.dimensions) for text in request.texts)
        return EmbeddingResult(
            vectors=vectors,
            provider="mock",
            model=self.model,
            dimensions=request.dimensions,
        )


class AzureOpenAIProvider(LLMProvider, EmbeddingProvider):
    """Azure OpenAI chat-completions and embeddings provider with lazy SDK import."""

    def __init__(
        self,
        *,
        api_key: str | None,
        endpoint: str | None,
        chat_deployment: str = "gpt-4o-mini",
        embedding_deployment: str = "text-embedding-3-small",
        api_version: str = "2024-10-21",
        client: Any | None = None,
    ) -> None:
        self._api_key = api_key
        self._endpoint = endpoint
        self._chat_deployment = chat_deployment
        self._embedding_deployment = embedding_deployment
        self._api_version = api_version
        self._client = client

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            from openai import AsyncAzureOpenAI  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "openai package is required for AzureOpenAIProvider when no client is injected"
            ) from exc

        self._client = AsyncAzureOpenAI(
            api_key=self._api_key,
            azure_endpoint=self._endpoint,
            api_version=self._api_version,
        )
        return self._client

    async def generate(self, request: GenerationRequest) -> GenerationResult:
        client = self._get_client()
        response = await client.chat.completions.create(
            model=self._chat_deployment,
            messages=_message_payload(request),
            temperature=request.temperature,
            max_tokens=request.max_tokens,
        )
        choice = _get_attr(response, "choices", [])[0]
        message = _get_attr(choice, "message", {})
        raw_text = _extract_text(_get_attr(message, "content", ""))
        payload = _parse_json_result(raw_text)
        result = _structured_from_payload(
            provider="azure_openai",
            model=self._chat_deployment,
            payload=payload,
            raw_text=raw_text,
        )
        guarded, _ = apply_guardrails(request, result)
        return guarded

    async def embed(self, request: EmbeddingRequest) -> EmbeddingResult:
        client = self._get_client()
        response = await client.embeddings.create(
            model=self._embedding_deployment,
            input=list(request.texts),
        )
        data = _get_attr(response, "data", [])
        vectors = tuple(tuple(float(x) for x in _get_attr(row, "embedding", ())) for row in data)
        return EmbeddingResult(
            vectors=vectors,
            provider="azure_openai",
            model=self._embedding_deployment,
            dimensions=len(vectors[0]) if vectors else request.dimensions,
        )


class AnthropicProvider(LLMProvider):
    """Anthropic Claude provider with lazy SDK import."""

    def __init__(
        self,
        *,
        api_key: str | None,
        model: str = "claude-3-5-sonnet-latest",
        client: Any | None = None,
    ) -> None:
        self._api_key = api_key
        self._model = model
        self._client = client

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            from anthropic import AsyncAnthropic  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "anthropic package is required for AnthropicProvider when no client is injected"
            ) from exc

        self._client = AsyncAnthropic(api_key=self._api_key)
        return self._client

    async def generate(self, request: GenerationRequest) -> GenerationResult:
        client = self._get_client()
        response = await client.messages.create(
            model=self._model,
            system=_anthropic_system_prompt(request),
            messages=_anthropic_message_payload(request),
            temperature=request.temperature,
            max_tokens=request.max_tokens,
        )
        raw_text = _extract_text(_get_attr(response, "content", ""))
        payload = _parse_json_result(raw_text)
        result = _structured_from_payload(
            provider="anthropic",
            model=self._model,
            payload=payload,
            raw_text=raw_text,
        )
        guarded, _ = apply_guardrails(request, result)
        return guarded
