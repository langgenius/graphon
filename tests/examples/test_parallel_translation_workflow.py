from __future__ import annotations

from io import StringIO

from examples.openai_slim_parallel_translation.workflow import (
    TranslationStreamWriter,
    write_stream_chunk,
)
from graphon.enums import BuiltinNodeTypes
from graphon.graph_events.node import NodeRunStreamChunkEvent


def test_write_stream_chunk_formats_translation_sections() -> None:
    output = StringIO()
    writer = TranslationStreamWriter(stream_output=output)

    zh_chunk = NodeRunStreamChunkEvent(
        id="event-1",
        node_id="translate_zh",
        node_type=BuiltinNodeTypes.LLM,
        selector=["translate_zh", "text"],
        chunk="你好",
        is_final=False,
    )
    zh_final = NodeRunStreamChunkEvent(
        id="event-2",
        node_id="translate_zh",
        node_type=BuiltinNodeTypes.LLM,
        selector=["translate_zh", "text"],
        chunk="",
        is_final=True,
    )
    en_chunk = NodeRunStreamChunkEvent(
        id="event-3",
        node_id="translate_en",
        node_type=BuiltinNodeTypes.LLM,
        selector=["translate_en", "text"],
        chunk="hello",
        is_final=False,
    )
    en_final = NodeRunStreamChunkEvent(
        id="event-4",
        node_id="translate_en",
        node_type=BuiltinNodeTypes.LLM,
        selector=["translate_en", "text"],
        chunk="",
        is_final=True,
    )

    assert write_stream_chunk(zh_chunk, stream_writer=writer) is True
    assert write_stream_chunk(zh_final, stream_writer=writer) is True
    assert write_stream_chunk(en_chunk, stream_writer=writer) is True
    assert write_stream_chunk(en_final, stream_writer=writer) is True

    assert output.getvalue() == "Chinese: 你好\nEnglish: hello\n"


def test_write_stream_chunk_ignores_non_translation_selectors() -> None:
    output = StringIO()
    writer = TranslationStreamWriter(stream_output=output)
    event = NodeRunStreamChunkEvent(
        id="event-5",
        node_id="output",
        node_type=BuiltinNodeTypes.END,
        selector=["output", "answer"],
        chunk="\n",
        is_final=False,
    )

    assert write_stream_chunk(event, stream_writer=writer) is False
    assert not output.getvalue()
