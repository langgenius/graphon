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
    _edge_decision_events,
    _format_selector_value,
    _GraphCase,
    _NodeCase,
    _OldPathResponseStream,
    _produce_node_variables,
)


@dataclass(frozen=True)
class _OptionalBranchCase:
    graph: _GraphCase
    selected_nodes: frozenset[int]
    skipped_nodes: frozenset[int]


@st.composite
def _optional_branch_cases(draw: st.DrawFn) -> _OptionalBranchCase:
    # Stable anchors are start=0, answer=1, condition=2, A=3, B=4, merge=5.
    extra_count = draw(st.integers(min_value=0, max_value=3))
    extra_sides = draw(
        st.lists(st.booleans(), min_size=extra_count, max_size=extra_count)
    )
    arms = [[3], [4]]
    for index, side in enumerate(extra_sides, 6):
        arms[int(side)].append(index)
    edges = [(0, 2), (2, 3), (2, 4), (5, 1)]
    for arm in arms:
        edges.extend(zip(arm, [*arm[1:], 5], strict=True))
    possible_shortcuts = [
        (tail, head)
        for arm in arms
        for position, tail in enumerate(arm)
        for head in [*arm[position + 1 :], 5]
    ]
    edges.extend(
        draw(st.lists(st.sampled_from(possible_shortcuts), max_size=18 - len(edges)))
    )
    nodes = [_NodeCase() for _ in range(6 + extra_count)]
    nodes[0] = _NodeCase(NodeExecutionType.ROOT)
    nodes[2] = _NodeCase(NodeExecutionType.BRANCH)
    for index in [*arms[0], *arms[1]]:
        selector_count = draw(st.integers(min_value=1, max_value=2))
        nodes[index] = _NodeCase(
            outputs=tuple(
                (f"n{index}", f"value{field}") for field in range(selector_count)
            )
        )
    references = [
        draw(st.sampled_from(nodes[3].outputs)),
        draw(st.sampled_from(nodes[4].outputs)),
    ]
    available_selectors = [
        selector for arm in arms for index in arm for selector in nodes[index].outputs
    ]
    references.extend(draw(st.lists(st.sampled_from(available_selectors), max_size=3)))
    nodes[1] = _NodeCase(
        NodeExecutionType.RESPONSE, references=tuple(draw(st.permutations(references)))
    )
    selected_arm_index = draw(st.integers(min_value=0, max_value=1))
    skipped_nodes = frozenset(arms[1 - selected_arm_index])
    decisions = [(0, True), (2 if selected_arm_index == 0 else 1, False)]
    for node in arms[1 - selected_arm_index]:
        decisions.extend(
            (index, False) for index, (tail, _) in enumerate(edges) if tail == node
        )
    decisions.append((1 if selected_arm_index == 0 else 2, True))
    for node in [*arms[selected_arm_index], 5]:
        outgoing = tuple(index for index, (tail, _) in enumerate(edges) if tail == node)
        decisions.extend((index, True) for index in draw(st.permutations(outgoing)))
    return _OptionalBranchCase(
        _GraphCase(tuple(nodes), tuple(edges), tuple(decisions)),
        frozenset(arms[selected_arm_index]),
        skipped_nodes,
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
            (item.node_id, tuple(item.selector), item.chunk, item.is_final)
            for item in chunks
        ] == old_path_stream.read_chunks()

    started = GraphRunStartedEvent()
    assert list(stream_filter.on_event(started)) == [started]
    assert_event_chunks(FrameStartedEvent(frame_id="root"))
    executed_nodes: set[int] = set()
    edge_decisions: dict[int, bool] = {}
    for event, (index, was_taken) in _edge_decision_events(case.graph, graph):
        tail, _ = case.graph.edges[index]
        if was_taken and tail not in executed_nodes:
            assert tail not in case.skipped_nodes
            incoming = [
                edge for edge, (_, head) in enumerate(case.graph.edges) if head == tail
            ]
            assert all(edge in edge_decisions for edge in incoming)
            assert tail == 0 or any(edge_decisions[edge] for edge in incoming)
            variables = _produce_node_variables(case.graph, graph, tail)
            if variables:
                old_path_stream.variable_values.update(
                    (selector, _format_selector_value(selector))
                    for selector in case.graph.nodes[tail].outputs
                )
                assert_event_chunks(
                    FrameVariablesUpdatedEvent(frame_id="root", variables=variables)
                )
            executed_nodes.add(tail)
        edge_decisions[index] = was_taken
        old_path_stream.apply_edge_decision(index, was_taken)
        assert_event_chunks(event)
    assert case.selected_nodes <= executed_nodes
    assert not case.skipped_nodes.intersection(executed_nodes)
    expected_stream = (
        "[n1]"
        + "".join(
            _format_selector_value(selector)
            for selector in case.graph.nodes[1].references
            if int(selector[0][1:]) in case.selected_nodes
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
