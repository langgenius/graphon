import json

import pytest
from pydantic import ValidationError

from graphon.engine.filter.builtin.response_stream import snapshot
from graphon.engine_events.node import NodeRunStreamChunkEvent


@pytest.mark.parametrize("version", ["1.0", "2.0"])
def test_snapshot_restores_legacy_buffered_chunk_identity(version: str) -> None:
    legacy_chunk = {
        "id": "source-run",
        "node_id": "source",
        "node_type": "llm",
        "selector": ["source", "answer"],
        "chunk": "buffered",
    }
    payload = {
        "version": version,
        "stream_buffers": [
            {"selector": ["source", "answer"], "events": [legacy_chunk] * 2},
        ],
    }
    state = snapshot.loads(json.dumps(payload))

    events = state.stream_buffers[0].events
    assert state.version == version
    assert [event.node_execution_id for event in events] == ["source-run"] * 2
    assert len({"source-run", *(event.id for event in events)}) == 3
    assert [event.chunk for event in events] == ["buffered"] * 2
    assert snapshot.loads(state.model_dump_json()) == state
    with pytest.raises(ValidationError, match="node_execution_id"):
        NodeRunStreamChunkEvent.model_validate(legacy_chunk)
    legacy_chunk["schema_version"] = "1.0"
    with pytest.raises(ValidationError, match="node_execution_id"):
        snapshot.loads(json.dumps(payload))


@pytest.mark.parametrize("version", ["1.0", "2.0"])
def test_snapshot_preserves_current_buffered_chunk_identity(version: str) -> None:
    chunk = NodeRunStreamChunkEvent(
        id="event-id",
        graph_id="graph-id",
        execution_id="execution-id",
        node_execution_id="source-run",
        node_id="source",
        node_type="llm",
        selector=["source", "answer"],
        chunk="buffered",
        sequence=7,
    )
    state = snapshot.loads(
        json.dumps({
            "version": version,
            "stream_buffers": [
                {
                    "selector": ["source", "answer"],
                    "events": [chunk.model_dump(mode="json")],
                },
            ],
        }),
    )

    assert state.stream_buffers[0].events == [chunk]
