from __future__ import annotations

from itertools import starmap
from typing import Any

import pytest

from graphon import dsl
from graphon.engine.filter import (
    EngineEventFilterContext,
    ResponseStreamFilter,
    filter_engine_events,
)
from graphon.engine_events.frame import FrameStartedEvent
from graphon.engine_events.graph import GraphRunSucceededEvent
from graphon.engine_events.node import NodeRunStreamChunkEvent, NodeRunSucceededEvent
from graphon.enums import NodeState
from graphon.graph import EdgeDefinition, Graph, GraphReference, NodeDefinition


def test_graph_owns_immutable_config_and_compares_by_content() -> None:
    selector = ["start", "value"]
    config: dict[str, Any] = {
        "answer": "ok",
        "outputs": [{"variable": "result", "value_selector": selector}],
    }
    finish = NodeDefinition("end", "end", config=config)
    start = NodeDefinition("start", "start")
    edge = EdgeDefinition("route", "start", "end")
    nodes = [start, finish]
    graph = Graph(nodes=nodes, edges=[edge])
    copy = Graph(
        nodes=[
            NodeDefinition("end", "end", config=config),
            NodeDefinition("start", "start"),
        ],
        edges=[EdgeDefinition("route", "start", "end")],
    )

    nodes.clear()
    selector.clear()
    assert graph == copy
    assert tuple(graph.node("end").config["outputs"][0]["value_selector"]) == (
        "start",
        "value",
    )
    assert graph != Graph(
        nodes=[
            start,
            NodeDefinition("end", "end", version="2", config=finish.config),
        ],
        edges=[edge],
    )
    assert graph != Graph(
        nodes=[start, NodeDefinition("end", "end", config={"outputs": []})],
        edges=[edge],
    )
    assert graph != Graph(
        nodes=[start, NodeDefinition("end", "answer", config=finish.config)],
        edges=[edge],
    )
    with pytest.raises(TypeError):
        graph.node("end").config["outputs"][0]["variable"] = "changed"
    with pytest.raises(TypeError):
        graph.nodes["new"] = start  # ty: ignore[invalid-assignment]
    with pytest.raises((AttributeError, TypeError)):
        graph.node("end").config["outputs"][0]["value_selector"].append("other")
    with pytest.raises((AttributeError, TypeError)):
        finish.version = "changed"  # ty: ignore[invalid-assignment]
    with pytest.raises((AttributeError, TypeError)):
        graph.nodes = {}  # ty: ignore[invalid-assignment]
    with pytest.raises((AttributeError, TypeError)):
        edge.source_handle = "changed"  # ty: ignore[invalid-assignment]


def test_only_outgoing_order_affects_graph_equality() -> None:
    nodes = [NodeDefinition(name, "start") for name in ("a", "b", "c", "d")]
    ab, ac, bd = (
        EdgeDefinition(src + dst, src, dst) for src, dst in ("ab", "ac", "bd")
    )
    graph = Graph(nodes=nodes, edges=[ab, ac, bd])

    assert graph.entry_node_ids == {"a", "b", "c", "d"}
    assert graph.outgoing("a") == (ab, ac)
    assert graph.incoming("d") == (bd,)
    assert graph == Graph(nodes=nodes, edges=[bd, ab, ac])
    assert graph != Graph(nodes=nodes, edges=[ac, ab, bd])
    assert graph != Graph(
        nodes=nodes, edges=[EdgeDefinition("ab", "a", "b", "yes"), ac, bd]
    )


@pytest.mark.parametrize(
    ("kinds", "routes"),
    [
        ([("a", "end")], []),
        ([("a", "start"), ("a", "start")], []),
        ([("a", "start")], [("x", "a", "absent")]),
        ([("a", "start")], [("x", "a", "a")]),
        (
            [("a", "start"), ("b", "end"), ("c", "end")],
            [("x", "a", "b"), ("x", "a", "c")],
        ),
    ],
    ids=["no-entry", "duplicate-node", "unknown-endpoint", "cycle", "duplicate-edge"],
)
def test_invalid_graphs_fail_at_construction(
    kinds: list[tuple[str, str]], routes: list[tuple[str, str, str]]
) -> None:
    nodes = [
        NodeDefinition(key, kind, config={"outputs": []} if kind == "end" else {})
        for key, kind in kinds
    ]
    edges = list(starmap(EdgeDefinition, routes))
    with pytest.raises(ValueError):  # ruff: ignore[pytest-raises-too-broad]
        Graph(nodes=nodes, edges=edges)


def test_shared_graph_runs_keep_their_paths_and_outputs() -> None:
    graph = dsl.inspect("""
kind: graph
graph:
  nodes:
    - {id: a, data: {type: start, variables: []}}
    - {id: b, data: {type: start, variables: []}}
    - id: left
      data: {type: end, outputs: [{variable: name, value_selector: [a, name]}]}
    - id: right
      data: {type: end, outputs: [{variable: name, value_selector: [b, name]}]}
  edges:
    - {id: first, source: a, target: left}
    - {id: second, source: b, target: right}
""").document.graph
    assert graph is not None
    left = dsl.load_graph(graph, root_node_id="a", start_inputs={"name": "Ada"})
    right = dsl.load_graph(graph, root_node_id="b", start_inputs={"name": "Lin"})

    another_left = dsl.load_graph(graph, root_node_id="a", start_inputs={"name": "Jo"})
    assert left.definition is right.definition is another_left.definition is graph
    assert graph.entry_node_ids == {"a", "b"}
    first_result = list(left.run())[-1]
    assert isinstance(first_result, GraphRunSucceededEvent)
    assert first_result.outputs == {"name": "Ada"}
    assert right.graph.nodes["right"].state == NodeState.UNKNOWN
    assert right.graph.edges["second"].state == NodeState.UNKNOWN
    assert another_left.graph.nodes["left"].state == NodeState.UNKNOWN
    assert another_left.graph.edges["first"].state == NodeState.UNKNOWN
    repeated_result = list(another_left.run())[-1]
    assert isinstance(repeated_result, GraphRunSucceededEvent)
    assert repeated_result.outputs == {"name": "Jo"}
    second_result = list(right.run())[-1]
    assert isinstance(second_result, GraphRunSucceededEvent)
    assert second_result.outputs == {"name": "Lin"}
    assert left.runtime_state.outputs == {"name": "Ada"}


def test_iteration_can_call_the_same_body_inline_or_by_reference() -> None:
    source = """
kind: graph
graph:
  nodes:
    - {id: start, data: {type: start, variables: []}}
    - id: each
      data:
        type: iteration
        start_node_id: enter
        iterator_selector: [start, items]
        output_selector: [render, output]
    - {id: enter, parentId: each, data: {type: iteration-start}}
    - id: render
      parentId: each
      data:
        type: template-transform
        variables: [{variable: item, value_selector: [each, item]}]
        template: 'hi {{ item }}'
    - id: end
      data: {type: end, outputs: [{variable: names, value_selector: [each, output]}]}
  edges:
    - {id: first, source: start, target: each}
    - {id: finish, source: each, target: end}
    - {id: first, source: enter, target: render}
"""
    graph = dsl.inspect(source).document.graph
    assert graph is not None
    call = graph.node("each")
    body = call.call_target
    assert isinstance(body, Graph)
    assert set(graph.nodes) == {"start", "each", "end"}
    assert set(graph.edges) == {"first", "finish"}
    assert set(body.edges) == {"first"}
    assert set(body.nodes) == {"enter", "render"}
    assert (
        graph
        == dsl.inspect(
            source.replace(
                "    - id: render\n",
                "    - id: render\n      position: {x: 50, y: 24}\n",
                1,
            )
        ).document.graph
    )
    assert (
        graph
        != dsl.inspect(source.replace("hi {{ item }}", "bye {{ item }}")).document.graph
    )

    def with_reference(target: GraphReference) -> Graph:
        referenced_call = NodeDefinition(
            call.id,
            call.node_type,
            version=call.version,
            config=call.config,
            call_target=target,
        )
        return Graph(
            nodes=[
                referenced_call if node.id == call.id else node
                for node in graph.nodes.values()
            ],
            edges=graph.edges.values(),
        )

    reference = GraphReference("greeter")
    by_reference = with_reference(reference)
    assert graph != by_reference
    assert by_reference == with_reference(GraphReference("greeter"))
    assert by_reference != with_reference(GraphReference("other"))
    inputs = {"items": ["Ada", "Lin"]}
    for engine in (
        dsl.loads(source, start_inputs=inputs),
        dsl.load_graph(graph, start_inputs=inputs),
    ):
        assert engine.definition == graph
        result = list(engine.run())[-1]
        assert isinstance(result, GraphRunSucceededEvent)
        assert result.outputs == {"names": ["hi Ada", "hi Lin"]}

    updated = dsl.inspect(
        source.replace("hi {{ item }}", "bye {{ item }}")
    ).document.graph
    assert updated is not None
    replacement = updated.node("each").call_target
    assert isinstance(replacement, Graph)
    referenced_engine = dsl.load_graph(
        by_reference,
        start_inputs=inputs,
        graph_resolver={reference: replacement}.__getitem__,
    )
    result = list(referenced_engine.run())[-1]
    assert isinstance(result, GraphRunSucceededEvent)
    assert result.outputs == {"names": ["bye Ada", "bye Lin"]}


def _two_loops(first: Graph, second: Graph | GraphReference, count: int) -> Graph:
    settings = {
        "start_node_id": "enter",
        "loop_count": count,
        "break_conditions": [],
        "logical_operator": "and",
    }
    return Graph(
        nodes=[
            NodeDefinition("start", "start"),
            NodeDefinition("first", "loop", config=settings, call_target=first),
            NodeDefinition("second", "loop", config=settings, call_target=second),
        ],
        edges=[
            EdgeDefinition("first-call", "start", "first"),
            EdgeDefinition("second-call", "first", "second"),
        ],
    )


def test_two_loops_can_call_bodies_with_the_same_local_node_ids() -> None:
    ref = GraphReference("body")
    body = Graph(
        nodes=[
            NodeDefinition("enter", "loop-start"),
            NodeDefinition("break", "loop-end"),
        ],
        edges=[EdgeDefinition("next", "enter", "break")],
    )
    graph = _two_loops(body, ref, 3)
    engine = dsl.load_graph(graph, graph_resolver={ref: body}.__getitem__)
    events = list(engine.run())
    assert isinstance(events[-1], GraphRunSucceededEvent)
    calls = [
        event.container_id
        for event in events
        if isinstance(event, FrameStartedEvent) and event.parent_frame_id is not None
    ]
    assert calls == ["first", "second"]


def test_responses_follow_the_called_graph_when_two_bodies_reuse_node_ids() -> None:
    def body(reply: str) -> Graph:
        return Graph(
            nodes=[
                NodeDefinition("enter", "loop-start"),
                NodeDefinition("reply", "answer", config={"answer": reply}),
            ],
            edges=[EdgeDefinition("next", "enter", "reply")],
        )

    graph = _two_loops(body("first reply"), body("second reply"), 1)
    engine = dsl.load_graph(graph)
    context = EngineEventFilterContext.from_engine(engine)
    events = list(
        filter_engine_events(
            list(engine.run()), filters=[ResponseStreamFilter()], context=context
        )
    )
    chunks = [
        event.chunk for event in events if isinstance(event, NodeRunStreamChunkEvent)
    ]
    assert "".join(chunks) == "first replysecond reply"


def test_canvas_layout_and_displayed_run_status_do_not_change_a_graph() -> None:
    source = """
kind: graph
graph:
  nodes:
    - id: start
      data: {type: start, variables: []}
  edges: []
"""
    edited = source.replace(
        "variables: []",
        """
        variables: [], width: 320, height: 220, position: {x: 40, y: 80},
        _runningStatus: succeeded, _connectedSourceHandleIds: [source]""",
    )
    original = dsl.inspect(source).document.graph
    moved = dsl.inspect(edited).document.graph
    assert original is not None
    assert moved is not None
    assert moved == original
    assert moved.node("start").config == original.node("start").config


def test_selecting_a_body_entry_skips_other_entries_before_their_paths_join() -> None:
    graph = Graph(
        nodes=[
            NodeDefinition("first", "loop-start"),
            NodeDefinition("second", "loop-start"),
            NodeDefinition("join", "end", config={"outputs": []}),
        ],
        edges=[
            EdgeDefinition("left", "first", "join"),
            EdgeDefinition("right", "second", "join"),
        ],
    )
    engine = dsl.load_graph(graph, root_node_id="first")
    assert graph.entry_node_ids == {"first", "second"}
    assert engine.graph.nodes["second"].state == NodeState.SKIPPED
    assert engine.graph.edges["right"].state == NodeState.SKIPPED
    events = list(engine.run())
    assert isinstance(events[-1], GraphRunSucceededEvent)
    assert any(
        isinstance(event, NodeRunSucceededEvent) and event.node_id == "join"
        for event in events
    )
