from typing import Any

from graphon.model_runtime.entities.llm_entities import (
    LLMResultChunk,
    LLMResultChunkDelta,
    LLMUsage,
)
from graphon.model_runtime.entities.message_entities import (
    AssistantPromptMessage,
    UserPromptMessage,
)
from graphon.model_runtime.model_providers.base.large_language_model import (
    _StreamingInvokeAccumulator,
)


def _make_chunk(
    *,
    content: str | None,
    opaque_body: Any | None = None,
    usage: LLMUsage | None = None,
) -> LLMResultChunk:
    message = AssistantPromptMessage(
        content=content,
        tool_calls=[],
        opaque_body=opaque_body,
    )
    delta = LLMResultChunkDelta(index=0, message=message, usage=usage)
    return LLMResultChunk(model="test-model", delta=delta)


def test_streaming_invoke_accumulator_preserves_opaque_body() -> None:
    accumulator = _StreamingInvokeAccumulator(real_model="test-model")
    opaque_body = {
        "assistant_blocks": [{"type": "thinking", "signature": "sig-1"}],
    }

    accumulator.consume(_make_chunk(content="hello"))
    accumulator.consume(_make_chunk(content=" world", opaque_body=opaque_body))

    result = accumulator.to_result(prompt_messages=[UserPromptMessage(content="hi")])

    assert result.message.opaque_body == opaque_body


def test_streaming_invoke_accumulator_opaque_body_defaults_to_none() -> None:
    accumulator = _StreamingInvokeAccumulator(real_model="test-model")

    accumulator.consume(_make_chunk(content="hello"))

    result = accumulator.to_result(prompt_messages=[UserPromptMessage(content="hi")])

    assert result.message.opaque_body is None


def test_streaming_invoke_accumulator_none_chunk_does_not_clobber_snapshot() -> None:
    accumulator = _StreamingInvokeAccumulator(real_model="test-model")
    opaque_body = {
        "assistant_blocks": [{"type": "redacted_thinking", "data": "enc-1"}],
    }

    accumulator.consume(_make_chunk(content="a", opaque_body=opaque_body))
    accumulator.consume(_make_chunk(content="b"))

    result = accumulator.to_result(prompt_messages=[UserPromptMessage(content="hi")])

    assert result.message.opaque_body == opaque_body
