"""Variables from mutually exclusive paths may be referenced after their merge."""

import json
from dataclasses import dataclass, replace

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from graphon.dsl import loads
from graphon.engine.filter import (
    EngineEventFilterContext,
    ResponseStreamFilter,
    filter_engine_events,
)
from graphon.engine_events.base import EngineEvent
from graphon.engine_events.frame import FrameStartedEvent, FrameVariablesUpdatedEvent
from graphon.engine_events.graph import GraphRunStartedEvent, GraphRunSucceededEvent
from graphon.engine_events.node import NodeRunStreamChunkEvent
from graphon.enums import NodeExecutionType
from tests.engine.test_response_stream_filter import _context
from tests.engine.test_response_stream_filter_differential import (
    _build_graph,
    _draw_edges,
    _edge_decision_events,
    _EdgeCase,
    _EdgeDecision,
    _format_selector_value,
    _GraphCase,
    _NodeCase,
    _OldPathResponseStream,
    _produce_node_variables,
    _ResponseChunk,
)


@dataclass(frozen=True, kw_only=True)
class _OptionalBranchCase:
    graph: _GraphCase
    selected_nodes: frozenset[str]
    skipped_nodes: frozenset[str]


@st.composite
def _optional_branch_cases(draw: st.DrawFn) -> _OptionalBranchCase:
    """Generate 6-9-node DAGs with two exclusive arms merging before one reply.

    Returns:
        A case with variable arm lengths, forward shortcuts, parallel edges,
        output selectors, and reference order. The reply references both arms,
        but only the selected arm executes. Rejected-arm edges are skipped and
        their producers supply no values.
    """
    extra_count = draw(st.integers(min_value=0, max_value=3))
    extra_sides = draw(
        st.lists(st.booleans(), min_size=extra_count, max_size=extra_count)
    )
    arms = [["a"], ["b"]]
    for index, side in enumerate(extra_sides):
        arms[int(side)].append(f"extra_{index}")
    edges = [
        _EdgeCase(edge_id="start_to_condition", source="start", target="condition"),
        _EdgeCase(edge_id="condition_to_a", source="condition", target="a"),
        _EdgeCase(edge_id="condition_to_b", source="condition", target="b"),
        _EdgeCase(edge_id="merge_to_answer", source="merge", target="answer"),
    ]
    for arm in arms:
        for source, target in zip(arm, [*arm[1:], "merge"], strict=True):
            edges.append(
                _EdgeCase(edge_id=f"edge_{len(edges)}", source=source, target=target)
            )
    for arm in arms:
        edges.extend(
            _draw_edges(
                draw, [*arm, "merge"], 18 - len(edges), first_edge_index=len(edges)
            )
        )
    nodes = {
        "start": _NodeCase(execution_type=NodeExecutionType.ROOT),
        "condition": _NodeCase(execution_type=NodeExecutionType.BRANCH),
        "merge": _NodeCase(),
    }
    for node_id in [*arms[0], *arms[1]]:
        selector_count = draw(st.integers(min_value=1, max_value=2))
        nodes[node_id] = _NodeCase(
            outputs=tuple((node_id, f"value{field}") for field in range(selector_count))
        )
    references = [
        draw(st.sampled_from(nodes["a"].outputs)),
        draw(st.sampled_from(nodes["b"].outputs)),
    ]
    available_selectors = [
        selector
        for arm in arms
        for node_id in arm
        for selector in nodes[node_id].outputs
    ]
    references.extend(draw(st.lists(st.sampled_from(available_selectors), max_size=3)))
    nodes["answer"] = _NodeCase(
        execution_type=NodeExecutionType.RESPONSE,
        references=tuple(draw(st.permutations(references))),
    )
    selected_arm_index = draw(st.integers(min_value=0, max_value=1))
    selected_arm = arms[selected_arm_index]
    skipped_arm = arms[1 - selected_arm_index]
    decisions = [
        _EdgeDecision(edge_id="start_to_condition", was_taken=True),
        _EdgeDecision(edge_id=f"condition_to_{skipped_arm[0]}", was_taken=False),
    ]
    for node_id in skipped_arm:
        decisions.extend(
            _EdgeDecision(edge_id=edge.edge_id, was_taken=False)
            for edge in edges
            if edge.source == node_id
        )
    decisions.append(
        _EdgeDecision(edge_id=f"condition_to_{selected_arm[0]}", was_taken=True)
    )
    for node_id in [*selected_arm, "merge"]:
        outgoing = tuple(edge.edge_id for edge in edges if edge.source == node_id)
        decisions.extend(
            _EdgeDecision(edge_id=edge_id, was_taken=True)
            for edge_id in draw(st.permutations(outgoing))
        )
    return _OptionalBranchCase(
        graph=_GraphCase(
            nodes=nodes, edges=tuple(edges), edge_decisions=tuple(decisions)
        ),
        selected_nodes=frozenset(selected_arm),
        skipped_nodes=frozenset(skipped_arm),
    )


def _assert_optional_branch_chunks_match_old_paths(case: _OptionalBranchCase) -> None:
    graph = _build_graph(case.graph)
    stream_filter = ResponseStreamFilter()
    stream_filter.initialize(replace(_context(graph), run_id="root-run"))
    old_path_stream = _OldPathResponseStream(case.graph)
    emitted_chunks: list[NodeRunStreamChunkEvent] = []

    def assert_event_chunks(event: EngineEvent) -> None:
        chunks = [
            item
            for item in stream_filter.on_event(event)
            if isinstance(item, NodeRunStreamChunkEvent)
        ]
        emitted_chunks.extend(chunks)
        assert [
            _ResponseChunk(
                node_id=item.node_id,
                selector=tuple(item.selector),
                chunk=item.chunk,
                is_final=item.is_final,
            )
            for item in chunks
        ] == old_path_stream.read_chunks()

    started = GraphRunStartedEvent()
    assert list(stream_filter.on_event(started)) == [started]
    assert_event_chunks(FrameStartedEvent(frame_id="root"))
    executed_nodes: set[str] = set()
    edge_decisions: dict[str, bool] = {}
    edges_by_id = {edge.edge_id: edge for edge in case.graph.edges}
    for edge_event in _edge_decision_events(case.graph, graph):
        decision = edge_event.decision
        source = edges_by_id[decision.edge_id].source
        if decision.was_taken and source not in executed_nodes:
            assert source not in case.skipped_nodes
            incoming = [edge for edge in case.graph.edges if edge.target == source]
            assert all(edge.edge_id in edge_decisions for edge in incoming)
            assert source == case.graph.root_node_id or any(
                edge_decisions[edge.edge_id] for edge in incoming
            )
            variables = _produce_node_variables(case.graph, graph, source)
            if variables:
                old_path_stream.variable_values.update(
                    (selector, _format_selector_value(selector))
                    for selector in case.graph.nodes[source].outputs
                )
                assert_event_chunks(
                    FrameVariablesUpdatedEvent(frame_id="root", variables=variables)
                )
            executed_nodes.add(source)
        edge_decisions[decision.edge_id] = decision.was_taken
        old_path_stream.apply_edge_decision(decision)
        assert_event_chunks(edge_event.event)
    assert case.selected_nodes <= executed_nodes
    assert not case.skipped_nodes.intersection(executed_nodes)
    expected_stream = (
        "[answer]"
        + "".join(
            _format_selector_value(selector)
            for selector in case.graph.nodes["answer"].references
            if selector[0] in case.selected_nodes
        )
        + ";"
    )
    assert "".join(item.chunk for item in emitted_chunks) == expected_stream
    assert emitted_chunks[-1].is_final
    assert sum(item.is_final for item in emitted_chunks) == 1


@settings(derandomize=True, deadline=None)
@given(case=_optional_branch_cases())
def test_optional_branch_variables_match_old_paths(case: _OptionalBranchCase) -> None:
    """Match legacy chunks for a merged reply referencing both branch outputs."""
    _assert_optional_branch_chunks_match_old_paths(case)


@pytest.mark.parametrize(
    ("selected_branch", "expected_stream"), [("a", "(A|)"), ("b", "(|B)")]
)
def test_optional_branches_preserve_streamed_output(
    selected_branch: str, expected_stream: str
) -> None:
    nodes = [
        {"id": "start", "data": {"type": "start", "variables": []}},
        {
            "id": "if",
            "data": {
                "type": "if-else",
                "cases": [
                    {
                        "case_id": "a",
                        "logical_operator": "and",
                        "conditions": [
                            {
                                "variable_selector": ["start", "choice"],
                                "comparison_operator": "is",
                                "value": "a",
                            }
                        ],
                    }
                ],
            },
        },
        *[
            {
                "id": node,
                "data": {
                    "type": "template-transform",
                    "template": text,
                    "variables": [],
                },
            }
            for node, text in [("a", "A"), ("b", "B"), ("c", "merge")]
        ],
        {
            "id": "answer",
            "data": {"type": "answer", "answer": "({{#a.output#}}|{{#b.output#}})"},
        },
    ]
    edges = [
        {"source": "start", "target": "if"},
        {"source": "if", "target": "a", "sourceHandle": "a"},
        {"source": "if", "target": "b", "sourceHandle": "false"},
        {"source": "a", "target": "c"},
        {"source": "b", "target": "c"},
        {"source": "c", "target": "answer"},
    ]
    engine = loads(
        json.dumps({"kind": "graph", "graph": {"nodes": nodes, "edges": edges}}),
        start_inputs={"choice": selected_branch},
        workers=1,
    )
    context = EngineEventFilterContext.from_engine(engine)
    engine_events = list(engine.run())
    assert isinstance(engine_events[-1], GraphRunSucceededEvent)
    chunks = [
        event
        for event in filter_engine_events(
            engine_events, context=context, filters=[ResponseStreamFilter()]
        )
        if isinstance(event, NodeRunStreamChunkEvent)
    ]
    assert "".join(event.chunk for event in chunks) == expected_stream
    assert chunks[-1].is_final
    assert sum(event.is_final for event in chunks) == 1
    skipped_node_id = "b" if selected_branch == "a" else "a"
    assert engine.runtime_state.variable_pool.get((skipped_node_id, "output")) is None
