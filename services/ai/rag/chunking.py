"""Deterministic semantic chunking over paragraphs and sentences."""

from __future__ import annotations

import re
from dataclasses import dataclass

from services.ai.rag.models import ChunkDocument, ChunkingConfig, SourceDocument

_SENTENCE_BOUNDARY = re.compile(r"(?<=[.!?])\s+")


@dataclass(frozen=True)
class _TextSegment:
    text: str
    start: int
    end: int
    overlap: bool = False


def count_tokens(text: str) -> int:
    """Return a lightweight deterministic token estimate."""
    return len(text.split())


def _normalize_text(text: str) -> str:
    return text.replace("\r\n", "\n").strip()


def _split_paragraphs(text: str) -> list[_TextSegment]:
    normalized = _normalize_text(text)
    if not normalized:
        return []

    paragraphs: list[_TextSegment] = []
    position = 0
    for part in re.split(r"(\n\s*\n)", normalized):
        if not part:
            continue
        if part.startswith("\n"):
            position += len(part)
            continue

        stripped = part.strip()
        if stripped:
            leading = len(part) - len(part.lstrip())
            start = position + leading
            paragraphs.append(
                _TextSegment(text=stripped, start=start, end=start + len(stripped))
            )
        position += len(part)
    return paragraphs


def _split_sentences(segment: _TextSegment) -> list[_TextSegment]:
    if not segment.text.strip():
        return []

    sentences: list[_TextSegment] = []
    search_from = 0
    for part in _SENTENCE_BOUNDARY.split(segment.text):
        stripped = part.strip()
        if not stripped:
            search_from += len(part)
            continue

        local_start = segment.text.find(stripped, search_from)
        if local_start == -1:
            local_start = segment.text.find(stripped)
        if local_start == -1:
            local_start = search_from
        local_end = local_start + len(stripped)
        search_from = local_end
        sentences.append(
            _TextSegment(
                text=stripped,
                start=segment.start + local_start,
                end=segment.start + local_end,
            )
        )
    return sentences or [segment]


def _split_long_sentence(
    sentence: _TextSegment,
    max_chars: int,
    max_tokens: int,
) -> list[_TextSegment]:
    word_matches = list(re.finditer(r"\S+", sentence.text))
    if not word_matches:
        return []

    pieces: list[_TextSegment] = []
    current: list[re.Match[str]] = []
    for word_match in word_matches:
        candidate_words = [match.group(0) for match in current] + [word_match.group(0)]
        candidate = " ".join(candidate_words)
        if current and (len(candidate) > max_chars or count_tokens(candidate) > max_tokens):
            pieces.append(
                _TextSegment(
                    text=" ".join(match.group(0) for match in current),
                    start=sentence.start + current[0].start(),
                    end=sentence.start + current[-1].end(),
                )
            )
            current = [word_match]
        else:
            current.append(word_match)

    if current:
        pieces.append(
            _TextSegment(
                text=" ".join(match.group(0) for match in current),
                start=sentence.start + current[0].start(),
                end=sentence.start + current[-1].end(),
            )
        )
    return pieces


def _split_segments(text: str, config: ChunkingConfig) -> list[_TextSegment]:
    segments: list[_TextSegment] = []
    for paragraph in _split_paragraphs(text):
        if (
            len(paragraph.text) <= config.max_chars
            and count_tokens(paragraph.text) <= config.max_tokens
        ):
            segments.append(paragraph)
            continue
        for sentence in _split_sentences(paragraph):
            if (
                len(sentence.text) <= config.max_chars
                and count_tokens(sentence.text) <= config.max_tokens
            ):
                segments.append(sentence)
            else:
                segments.extend(
                    _split_long_sentence(sentence, config.max_chars, config.max_tokens)
                )
    return segments


def _overlap_text(text: str, config: ChunkingConfig) -> str:
    if not text:
        return ""

    words = text.split()
    if not words:
        return ""

    overlap_words: list[str] = []
    for word in reversed(words):
        candidate_words = [word] + overlap_words
        candidate = " ".join(candidate_words)
        if config.overlap_tokens and len(candidate_words) > config.overlap_tokens:
            break
        if config.overlap_chars and len(candidate) > config.overlap_chars:
            break
        overlap_words = candidate_words
    return " ".join(overlap_words).strip()


def _join_segments(segments: list[_TextSegment]) -> str:
    return " ".join(segment.text.strip() for segment in segments if segment.text.strip()).strip()


def _within_limits(text: str, config: ChunkingConfig) -> bool:
    return len(text) <= config.max_chars and count_tokens(text) <= config.max_tokens


def _validate_config(config: ChunkingConfig) -> None:
    if config.max_chars <= 0:
        raise ValueError("max_chars must be positive")
    if config.max_tokens <= 0:
        raise ValueError("max_tokens must be positive")
    if config.overlap_chars < 0:
        raise ValueError("overlap_chars must be non-negative")
    if config.overlap_tokens < 0:
        raise ValueError("overlap_tokens must be non-negative")


def _overlap_segment(
    chunk_text: str,
    chunk_segments: list[_TextSegment],
    config: ChunkingConfig,
) -> _TextSegment | None:
    overlap = _overlap_text(chunk_text, config)
    if not overlap or overlap == chunk_text:
        return None

    source_start = chunk_segments[0].start
    source_end = chunk_segments[-1].end
    start = max(source_start, source_end - len(overlap))
    return _TextSegment(text=overlap, start=start, end=source_end, overlap=True)


def chunk_document(
    document: SourceDocument,
    config: ChunkingConfig | None = None,
) -> tuple[ChunkDocument, ...]:
    """Split a document into deterministic semantic chunks with overlap."""
    resolved = config or ChunkingConfig()
    _validate_config(resolved)
    text = _normalize_text(document.text)
    if not text:
        return ()

    segments = _split_segments(text, resolved)
    if not segments:
        return ()

    chunks: list[ChunkDocument] = []
    current_segments: list[_TextSegment] = []

    def flush() -> None:
        nonlocal current_segments
        if not current_segments:
            return

        chunk_text = _join_segments(current_segments)
        if not chunk_text:
            current_segments = []
            return

        start_char = min(segment.start for segment in current_segments)
        end_char = max(segment.end for segment in current_segments)

        metadata = dict(document.metadata)
        metadata.setdefault("citation", document.citation)
        chunks.append(
            ChunkDocument(
                chunk_id=f"{document.doc_id}::chunk-{len(chunks)}",
                source_doc_id=document.doc_id,
                chunk_index=len(chunks),
                text=chunk_text,
                title=document.title,
                url=document.url,
                metadata=metadata,
                start_char=start_char,
                end_char=end_char,
                token_count=count_tokens(chunk_text),
            )
        )

        overlap = _overlap_segment(chunk_text, current_segments, resolved)
        current_segments = [overlap] if overlap is not None else []

    for segment in segments:
        candidate_segments = current_segments + [segment]
        candidate_text = _join_segments(candidate_segments)

        if current_segments and not _within_limits(candidate_text, resolved):
            if all(part.overlap for part in current_segments):
                candidate_segments = [segment]
            else:
                flush()
                candidate_segments = current_segments + [segment]
                candidate_text = _join_segments(candidate_segments)
                if current_segments and not _within_limits(candidate_text, resolved):
                    candidate_segments = [segment]

        candidate_text = _join_segments(candidate_segments)
        if current_segments and not _within_limits(candidate_text, resolved):
            flush()
            candidate_segments = current_segments + [segment]
            candidate_text = _join_segments(candidate_segments)
            if current_segments and not _within_limits(candidate_text, resolved):
                candidate_segments = [segment]
                candidate_text = _join_segments(candidate_segments)

        if candidate_text:
            current_segments = candidate_segments

    flush()
    return tuple(chunks)
