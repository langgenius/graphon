"""Ordered streaming before later gates, including interleaved frame targets."""

from graphon.engine.filter import ResponseStreamFilter
from graphon.engine_events.frame import FrameStartedEvent
from graphon.engine_events.graph import GraphRunStartedEvent
from graphon.engine_events.node import NodeRunStreamChunkEvent
from graphon.enums import BuiltinNodeTypes, NodeExecutionType
from graphon.nodes.base.template import Template, TextSegment
from tests.engine.test_response_stream_filter import (
    _context,
    _edge_taken,
    _TestEdge,
    _TestGraph,
    _TestNode,
)


def test_first_response_precedes_the_last_blocking_edge() -> None:
    graph = _TestGraph(
        nodes={
            "root": _TestNode("root", execution_type=NodeExecutionType.ROOT),
            "first": _TestNode(
                "first",
                execution_type=NodeExecutionType.RESPONSE,
                template=Template(segments=[TextSegment(text="first")]),
            ),
            "last": _TestNode(
                "last",
                execution_type=NodeExecutionType.RESPONSE,
                template=Template(segments=[TextSegment(text="last")]),
            ),
        },
        edges={
            "nonblocking": _TestEdge("nonblocking", "root", "first"),
            "blocking": _TestEdge("blocking", "first", "last"),
        },
        root_node_id="root",
    )
    stream_filter = ResponseStreamFilter()
    stream_filter.initialize(_context(graph))
    assert list(
        stream_filter.on_event(
            FrameStartedEvent(
                frame_id="root",
                node_execution_ids={"first": "first-run", "last": "last-run"},
            )
        )
    ) == [
        NodeRunStreamChunkEvent(
            id="first-run",
            node_id="first",
            node_type=BuiltinNodeTypes.END,
            selector=["first", "answer"],
            chunk="first",
            is_final=True,
        )
    ]
    # The first response is visible before either edge event. In particular,
    # the downstream response's last blocking edge has not been satisfied.
    assert (
        list(
            stream_filter.on_event(
                _edge_taken(edge_id="nonblocking", source_id="root", answer_id="first")
            )
        )
        == []
    )
    assert list(
        stream_filter.on_event(
            _edge_taken(edge_id="blocking", source_id="first", answer_id="last")
        )
    ) == [
        NodeRunStreamChunkEvent(
            id="last-run",
            node_id="last",
            node_type=BuiltinNodeTypes.END,
            selector=["last", "answer"],
            chunk="last",
            is_final=True,
        )
    ]
    assert list(stream_filter.flush()) == []


def test_interleaved_frames_gate_distinct_response_targets_independently() -> None:
    graph_config = {
        "nodes": [
            {"id": "container", "data": {"type": "iteration"}},
            *[
                {
                    "id": node_id,
                    "data": {
                        "type": node_type,
                        "container_id": "container",
                        **({"answer": node_id} if node_type == "answer" else {}),
                    },
                }
                for node_id, node_type in [
                    ("start", "iteration-start"),
                    ("gate", "if-else"),
                    ("tail", "iteration-start"),
                    ("reply-a", "answer"),
                    ("reply-b", "answer"),
                ]
            ],
        ],
        "edges": [
            {"id": edge_id, "source": source, "target": target}
            for edge_id, source, target in [
                ("start-gate", "start", "gate"),
                ("open", "gate", "tail"),
                ("tail-a", "tail", "reply-a"),
                ("tail-b", "tail", "reply-b"),
            ]
        ],
    }
    graph = _TestGraph(
        nodes={"root": _TestNode("root", execution_type=NodeExecutionType.ROOT)},
        edges={},
        root_node_id="root",
        graph_config=graph_config,
    )
    stream_filter = ResponseStreamFilter()
    stream_filter.initialize(_context(graph))
    started = GraphRunStartedEvent()
    assert list(stream_filter.on_event(started)) == [started]
    for frame_id, reply_node_id in [("a", "reply-a"), ("b", "reply-b")]:
        assert (
            list(
                stream_filter.on_event(
                    FrameStartedEvent(
                        frame_id=frame_id,
                        parent_frame_id="root",
                        container_id="container",
                        root_node_id="start",
                        node_execution_ids={
                            node_id: f"{frame_id}-{node_id}"
                            for node_id in ["start", "gate", "tail", reply_node_id]
                        },
                    )
                )
            )
            == []
        )
    # Identical local edge IDs are independently consumed. B opens first;
    # taking B's edge twice must neither open A nor replay B's reply.
    for frame_id, reply_node_id in [("b", "reply-b"), ("a", "reply-a")]:
        event = _edge_taken(
            edge_id="open", source_id="gate", answer_id="tail", frame_id=frame_id
        )
        assert list(stream_filter.on_event(event)) == [
            NodeRunStreamChunkEvent(
                id=f"{frame_id}-{reply_node_id}",
                frame_id=frame_id,
                container_id="container",
                node_id=reply_node_id,
                node_type=BuiltinNodeTypes.ANSWER,
                selector=[reply_node_id, "answer"],
                chunk=reply_node_id,
            ),
            NodeRunStreamChunkEvent(
                id=f"{frame_id}-{reply_node_id}",
                frame_id=frame_id,
                container_id="container",
                node_id=reply_node_id,
                node_type=BuiltinNodeTypes.ANSWER,
                selector=[reply_node_id, "answer"],
                chunk="",
                is_final=True,
            ),
        ]
        assert list(stream_filter.on_event(event)) == []
    assert list(stream_filter.flush()) == []
