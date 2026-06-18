"""Lightweight groundedness and low-confidence guardrails for AI outputs."""

from __future__ import annotations

import re
from dataclasses import replace

from services.ai.llm.models import GenerationRequest, GenerationResult, GuardrailReport

REFUSAL_PATTERNS = (
    "i can't help",
    "i cannot help",
    "i don't have enough information",
    "i do not have enough information",
    "insufficient context",
    "not enough context",
)
TOKEN_RE = re.compile(r"[a-z0-9]{4,}")


def _tokenize(text: str) -> set[str]:
    return set(TOKEN_RE.findall(text.lower()))


def _context_tokens(request: GenerationRequest) -> set[str]:
    tokens: set[str] = set()
    for document in request.context:
        tokens.update(_tokenize(document.title or ""))
        tokens.update(_tokenize(document.text))
    return tokens


def _output_tokens(result: GenerationResult) -> set[str]:
    return _tokenize(f"{result.summary} {result.explanation}")


def evaluate_result(
    request: GenerationRequest,
    result: GenerationResult,
    *,
    min_confidence: float = 0.45,
    min_overlap_ratio: float = 0.08,
) -> GuardrailReport:
    """Evaluate whether a generation is grounded enough for downstream use."""

    text = f"{result.summary} {result.explanation}".strip()
    empty_output = not text
    refusal_detected = any(pattern in text.lower() for pattern in REFUSAL_PATTERNS)
    confidence_ok = result.confidence >= min_confidence

    allowed_citations = {doc.citation for doc in request.context}
    citations_ok = bool(result.citations) and all(
        citation in allowed_citations for citation in result.citations
    )

    context_tokens = _context_tokens(request)
    output_tokens = _output_tokens(result)
    overlap_count = len(context_tokens & output_tokens)
    overlap_ratio = overlap_count / max(1, len(output_tokens))
    overlap_ok = overlap_ratio >= min_overlap_ratio if request.context else True

    grounded = bool(request.context) and citations_ok and overlap_ok and not refusal_detected
    accepted = grounded and confidence_ok and not empty_output

    reasons: list[str] = []
    if empty_output:
        reasons.append("empty_output")
    if refusal_detected:
        reasons.append("refusal_detected")
    if not confidence_ok:
        reasons.append("low_confidence")
    if not citations_ok:
        reasons.append("citation_mismatch")
    if not overlap_ok:
        reasons.append("context_overlap_too_low")
    if not request.context:
        reasons.append("no_context")

    return GuardrailReport(
        accepted=accepted,
        grounded=grounded,
        confidence_ok=confidence_ok,
        citations_ok=citations_ok,
        overlap_ok=overlap_ok,
        refusal_detected=refusal_detected,
        empty_output=empty_output,
        reasons=tuple(reasons),
    )


def apply_guardrails(
    request: GenerationRequest,
    result: GenerationResult,
    *,
    min_confidence: float = 0.45,
    min_overlap_ratio: float = 0.08,
) -> tuple[GenerationResult, GuardrailReport]:
    """Return a result updated with groundedness flags plus its evaluation report."""

    report = evaluate_result(
        request,
        result,
        min_confidence=min_confidence,
        min_overlap_ratio=min_overlap_ratio,
    )
    guarded = replace(
        result,
        grounded=report.grounded,
        metadata={**result.metadata, "guardrail_reasons": list(report.reasons)},
    )
    return guarded, report

