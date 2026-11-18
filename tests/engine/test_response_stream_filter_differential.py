"""Separate small-DAG laws for traversal gates and executable variable dependencies."""

from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass, replace
from typing import cast, override

from hypothesis import example, given, settings
from hypothesis import strategies as st

from graphon.engine.filter import ResponseStreamFilter
from graphon.engine_events.base import EngineEvent
from graphon.engine_events.frame import FrameStartedEvent, FrameVariablesUpdatedEvent
from graphon.engine_events.graph import GraphRunStartedEvent
from graphon.engine_events.node import NodeRunStreamChunkEvent
from graphon.engine_events.traversal import GraphEdgeSkippedEvent
from graphon.enums import NodeExecutionType, WorkflowNodeExecutionStatus
from graphon.node_events.node import StreamCompletedEvent, VariableUpdatedEvent
from graphon.nodes.base.template import Template, TextSegment, VariableSegment
from graphon.nodes.variable_assigner.v2.entities import (
    VariableAssignerNodeData,
    VariableOperationItem,
)
from graphon.nodes.variable_assigner.v2.enums import InputType, Operation
from graphon.nodes.variable_assigner.v2.node import VariableAssignerNode
from graphon.runtime.runtime_state import NodeProtocol, RuntimeState
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.variables import StringVariable, Variable
from tests.engine.test_response_stream_filter import (
    _context,
    _edge_taken,
    _TestEdge,
    _TestGraph,
    _TestNode,
)
from tests.helpers import build_init_params

type _Selector = tuple[str, str]
type _ResponseChunk = tuple[str, tuple[str, ...], str, bool]


@dataclass(frozen=True)
class _NodeCase:
    execution_type: NodeExecutionType = NodeExecutionType.EXECUTABLE
    outputs: tuple[_Selector, ...] = ()
    assignments: tuple[_Selector, ...] = ()
    references: tuple[_Selector, ...] = ()


@dataclass(frozen=True)
class _GraphCase:
    nodes: tuple[_NodeCase, ...]
    edges: tuple[tuple[int, int], ...]
    edge_decisions: tuple[tuple[int, bool], ...] = ()
    execution_order: tuple[int, ...] = ()


class _NodeWithoutVariableBlocking(_TestNode):
    @override
    def blocks_variable_output(self, variable_selectors: set[tuple[str, ...]]) -> bool:
        # Match Node's default: owning an output does not make a node a blocker.
        return False


def _draw_edges(
    draw: st.DrawFn, order: list[int], max_size: int
) -> list[tuple[int, int]]:
    indices = st.integers(min_value=0, max_value=len(order) - 1)
    ranks = {node: index for index, node in enumerate(order)}
    return [
        (tail, head) if ranks[tail] < ranks[head] else (head, tail)
        for tail, head in draw(st.lists(st.tuples(indices, indices), max_size=max_size))
        if tail != head
    ]


@st.composite
def _gate_cases(draw: st.DrawFn, gate_type: NodeExecutionType) -> _GraphCase:
    node_count = draw(
        st.integers(
            min_value=3 if gate_type == NodeExecutionType.RESPONSE else 1, max_value=9
        )
    )
    # Stable root/terminal IDs allow unrelated vertices to shrink away.
    order = [0, *range(2, node_count), 1][:node_count]
    edges = _draw_edges(draw, order, 18)
    nodes = [
        _NodeCase(NodeExecutionType.ROOT),
        *[_NodeCase() for _ in range(node_count - 1)],
    ]
    nodes[min(1, node_count - 1)] = _NodeCase(NodeExecutionType.RESPONSE)
    gates = (
        draw(
            st.sets(
                st.integers(min_value=2, max_value=node_count - 1),
                min_size=1 if gate_type == NodeExecutionType.RESPONSE else 0,
            )
        )
        if node_count > 2
        else set()
    )
    for index in gates:
        nodes[index] = _NodeCase(gate_type)
    decisions = (
        draw(
            st.lists(
                st.tuples(
                    st.integers(min_value=0, max_value=len(edges) - 1), st.booleans()
                ),
                max_size=2 * len(edges) + 6,
            )
        )
        if edges
        else []
    )
    return _GraphCase(tuple(nodes), tuple(edges), tuple(decisions))


def _draw_rooted_edges(
    draw: st.DrawFn, order: list[int], required_parents: dict[int, int]
) -> list[tuple[int, int]]:
    edges = [
        (
            required_parents[node]
            if node in required_parents
            else draw(st.sampled_from(order[:position])),
            node,
        )
        for position, node in enumerate(order[1:], 1)
    ]
    edges.extend(
        (tail, head)
        for tail, head in _draw_edges(draw, order, 18 - len(edges))
        if head not in required_parents or required_parents[head] == tail
    )
    return edges


def _find_dominators(
    order: list[int], edges: list[tuple[int, int]]
) -> dict[int, set[int]]:
    dominators_by_node = {0: {0}}
    for node in order[1:]:
        dominators_by_node[node] = {node} | set.intersection(
            *(dominators_by_node[tail] for tail, head in edges if head == node)
        )
    return dominators_by_node


def _draw_execution_case(
    draw: st.DrawFn, nodes: list[_NodeCase], edges: list[tuple[int, int]]
) -> _GraphCase:
    pending = set(range(len(nodes)))
    order = []
    while pending:
        ready = sorted(
            node
            for node in pending
            if all(tail not in pending for tail, head in edges if head == node)
        )
        node = draw(st.sampled_from(ready))
        pending.remove(node)
        order.append(node)
    edge_order = draw(st.permutations(tuple(range(len(edges)))))
    repeats = draw(
        st.lists(st.integers(min_value=0, max_value=len(edges) - 1), max_size=3)
    )
    return _GraphCase(
        tuple(nodes),
        tuple(edges),
        tuple((index, True) for index in (*edge_order, *repeats)),
        tuple(order),
    )


@st.composite
def _variable_dependency_cases(draw: st.DrawFn, *, assignments: bool) -> _GraphCase:
    node_count = draw(st.integers(min_value=3, max_value=9))
    order = [0, *range(2, node_count), 1]
    parent = draw(st.integers(min_value=2, max_value=node_count - 1))
    edges = _draw_rooted_edges(draw, order, {1: parent})
    dominators = _find_dominators(order, edges)[1] - {0, 1}
    nodes = [
        _NodeCase(NodeExecutionType.ROOT),
        *[_NodeCase() for _ in range(node_count - 1)],
    ]
    for index in range(2, node_count):
        selector_count = draw(
            st.integers(min_value=1 if index == parent else 0, max_value=3)
        )
        selectors = tuple(
            ("conversation", f"v{index}_{field}")
            if assignments
            else (f"n{index}", f"value{field}")
            for field in range(selector_count)
        )
        nodes[index] = (
            _NodeCase(assignments=selectors)
            if assignments
            else _NodeCase(outputs=selectors)
        )
    available_selectors = [
        selector
        for index in sorted(dominators)
        for selector in (*nodes[index].outputs, *nodes[index].assignments)
    ]
    references = draw(
        st.lists(
            st.sampled_from(available_selectors),
            min_size=1,
            max_size=min(3, len(available_selectors)),
            unique=True,
        )
    )
    nodes[1] = _NodeCase(NodeExecutionType.RESPONSE, references=tuple(references))
    return _draw_execution_case(draw, nodes, edges)


@st.composite
def _distinct_response_dependency_cases(draw: st.DrawFn) -> _GraphCase:
    node_count = draw(st.integers(min_value=5, max_value=9))
    order = [0, 3, *range(5, node_count), 4, 2, 1]
    # Both responses share the writers' path, but only the second must wait
    # for writer 4. Unioning their dependencies would delay the first reply.
    last_parent = draw(st.sampled_from([4, 2]))
    edges = _draw_rooted_edges(draw, order, {4: 3, 2: 4, 1: last_parent})
    dominators = _find_dominators(order, edges)
    nodes = [
        _NodeCase(NodeExecutionType.ROOT),
        *[_NodeCase() for _ in range(node_count - 1)],
    ]
    for index in [3, 4, *range(5, node_count)]:
        selector_count = draw(
            st.integers(min_value=1 if index in {3, 4} else 0, max_value=2)
        )
        nodes[index] = _NodeCase(
            assignments=tuple(
                ("conversation", f"v{index}_{field}") for field in range(selector_count)
            )
        )
    for response, writer in [(2, 3), (1, 4)]:
        required_selector = draw(st.sampled_from(nodes[writer].assignments))
        available_selectors = (
            list(nodes[writer].assignments)
            if response == 2
            else [
                selector
                for index in sorted(dominators[response] - {response})
                for selector in nodes[index].assignments
            ]
        )
        extra_references = draw(
            st.lists(st.sampled_from(available_selectors), max_size=2, unique=True)
        )
        nodes[response] = _NodeCase(
            NodeExecutionType.RESPONSE,
            references=tuple(dict.fromkeys([required_selector, *extra_references])),
        )
    return _draw_execution_case(draw, nodes, edges)


def _enumerate_old_blocking_paths(case: _GraphCase, response: int) -> list[list[int]]:
    references = set(case.nodes[response].references)
    blockers = {
        index
        for index, node in enumerate(case.nodes)
        if node.execution_type in {NodeExecutionType.BRANCH, NodeExecutionType.RESPONSE}
        or references.intersection(node.assignments)
    }

    def enumerate_paths(node: int, path: list[int]) -> list[list[int]]:
        if node == response:
            return [[index for index in path if case.edges[index][0] in blockers]]
        return [
            complete
            for index, (tail, head) in enumerate(case.edges)
            if tail == node
            for complete in enumerate_paths(head, [*path, index])
        ]

    return enumerate_paths(0, [])


def _format_selector_value(selector: _Selector) -> str:
    return ":".join(selector)


def _build_initial_variables(case: _GraphCase) -> tuple[Variable, ...]:
    return tuple(
        StringVariable(
            name=selector[1],
            selector=selector,
            value=f"old:{_format_selector_value(selector)}",
        )
        for node in case.nodes
        for selector in node.assignments
    )


def _build_graph(case: _GraphCase) -> _TestGraph:
    pool = VariablePool()
    for variable in _build_initial_variables(case):
        pool.add(variable.selector, variable)
    runtime_state = RuntimeState(workflow_id="workflow", variable_pool=pool, start_at=0)
    nodes: dict[str, NodeProtocol] = {}
    for index, node in enumerate(case.nodes):
        node_id = f"n{index}"
        if node.assignments:
            nodes[node_id] = VariableAssignerNode(
                node_id,
                VariableAssignerNodeData(
                    title="Assign values",
                    items=[
                        VariableOperationItem(
                            variable_selector=selector,
                            input_type=InputType.CONSTANT,
                            operation=Operation.OVER_WRITE,
                            value=_format_selector_value(selector),
                        )
                        for selector in node.assignments
                    ],
                ),
                init_params=build_init_params(graph_config={"nodes": [], "edges": []}),
                runtime_state=runtime_state,
            )
        else:
            nodes[node_id] = _NodeWithoutVariableBlocking(
                node_id,
                execution_type=node.execution_type,
                template=Template(
                    segments=[
                        TextSegment(text=f"[{node_id}]"),
                        *[
                            VariableSegment(selector=selector)
                            for selector in node.references
                        ],
                        TextSegment(text=";"),
                    ]
                ),
            )
    return _TestGraph(
        nodes=cast(dict[str, _TestNode], nodes),
        edges={
            str(index): _TestEdge(str(index), f"n{tail}", f"n{head}")
            for index, (tail, head) in enumerate(case.edges)
        },
        root_node_id="n0",
    )


class _OldPathResponseStream:
    def __init__(self, case: _GraphCase) -> None:
        self.case = case
        self.blocking_paths = {
            index: _enumerate_old_blocking_paths(case, index)
            for index, node in enumerate(case.nodes)
            if node.execution_type == NodeExecutionType.RESPONSE
        }
        self.variable_values = {
            cast(_Selector, tuple(variable.selector)): str(variable.value)
            for variable in _build_initial_variables(case)
        }
        self.started_responses: set[int] = set()
        self.waiting_responses: deque[int] = deque()
        self.segment_positions: dict[int, int] = dict.fromkeys(self.blocking_paths, 0)
        self.skipped_edge_ids: set[int] = set()

    def apply_edge_decision(self, index: int, was_taken: bool) -> None:
        if not was_taken:
            self.skipped_edge_ids.add(index)
        for response, paths in self.blocking_paths.items():
            if was_taken:
                for path in paths:
                    if index in path:
                        path.remove(index)
            else:
                self.blocking_paths[response] = [
                    path for path in paths if index not in path
                ]

    def _source_was_skipped(self, selector: _Selector) -> bool:
        node_indices = {f"n{index}": index for index in range(len(self.case.nodes))}
        if selector[0] not in node_indices:
            return False
        # Walk backwards to the root independently of the filter's forward skip
        # scan. Missing data alone does not make a source skipped.
        pending = [node_indices[selector[0]]]
        visited: set[int] = set()
        while pending:
            node = pending.pop()
            if node == 0:
                return False
            if node in visited:
                continue
            visited.add(node)
            pending.extend(
                tail
                for index, (tail, head) in enumerate(self.case.edges)
                if head == node and index not in self.skipped_edge_ids
            )
        return True

    def read_chunks(self) -> list[_ResponseChunk]:
        for response, paths in sorted(self.blocking_paths.items()):
            if response not in self.started_responses and any(
                not path for path in paths
            ):
                self.started_responses.add(response)
                self.waiting_responses.append(response)
        chunks: list[_ResponseChunk] = []
        while self.waiting_responses:
            response = self.waiting_responses[0]
            response_id = f"n{response}"
            references = self.case.nodes[response].references
            position = self.segment_positions[response]
            if position == 0:
                chunks.append((
                    response_id,
                    (response_id, "answer"),
                    f"[{response_id}]",
                    False,
                ))
            elif position <= len(references):
                selector = references[position - 1]
                if self._source_was_skipped(selector):
                    self.segment_positions[response] += 1
                    continue
                if selector not in self.variable_values:
                    return chunks
                source = selector[0] if selector[0].startswith("n") else response_id
                chunks.append((source, selector, self.variable_values[selector], False))
            else:
                chunks.append((response_id, (response_id, "answer"), ";", True))
                self.waiting_responses.popleft()
            self.segment_positions[response] += 1
        return chunks


def _edge_decision_events(
    case: _GraphCase, graph: _TestGraph, node: int | None = None
) -> Iterator[tuple[EngineEvent, tuple[int, bool]]]:
    for index, was_taken in case.edge_decisions:
        tail, _ = case.edges[index]
        if node is not None and tail != node:
            continue
        edge = graph.edges[str(index)]
        event = (
            _edge_taken(edge_id=edge.id, source_id=edge.tail, answer_id=edge.head)
            if was_taken
            else GraphEdgeSkippedEvent(
                frame_id="root",
                edge_id=edge.id,
                source_node_id=edge.tail,
                target_node_id=edge.head,
            )
        )
        yield event, (index, was_taken)


def _produce_node_variables(
    case: _GraphCase, graph: _TestGraph, index: int
) -> tuple[Variable, ...]:
    node = case.nodes[index]
    if not node.assignments:
        return tuple(
            StringVariable(
                name=selector[1],
                selector=selector,
                value=_format_selector_value(selector),
            )
            for selector in node.outputs
        )
    assigner = cast(VariableAssignerNode, graph.nodes[f"n{index}"])
    updates = []
    for event in assigner._run():
        if isinstance(event, VariableUpdatedEvent):
            updates.append(event.variable)
        elif isinstance(event, StreamCompletedEvent):
            assert event.node_run_result.status == WorkflowNodeExecutionStatus.SUCCEEDED
    assert {tuple(variable.selector) for variable in updates} == set(node.assignments)
    for variable in updates:
        assigner.runtime_state.variable_pool.add(variable.selector, variable)
    return tuple(updates)


def _assert_response_chunks_match_old_paths(case: _GraphCase) -> None:
    graph = _build_graph(case)
    stream_filter = ResponseStreamFilter()
    stream_filter.initialize(replace(_context(graph), run_id="root-run"))
    old_path_stream = _OldPathResponseStream(case)

    def assert_event_chunks(event: EngineEvent) -> None:
        actual_chunks = [
            (item.node_id, tuple(item.selector), item.chunk, item.is_final)
            for item in stream_filter.on_event(event)
            if isinstance(item, NodeRunStreamChunkEvent)
        ]
        assert actual_chunks == old_path_stream.read_chunks()

    started = GraphRunStartedEvent()
    assert list(stream_filter.on_event(started)) == [started]
    assert_event_chunks(
        FrameStartedEvent(frame_id="root", variables=_build_initial_variables(case))
    )
    if not case.execution_order:
        for event, decision in _edge_decision_events(case, graph):
            old_path_stream.apply_edge_decision(*decision)
            assert_event_chunks(event)
        return

    completed_nodes: set[int] = set()
    committed_selectors: set[_Selector] = set()
    for index in case.execution_order:
        node = case.nodes[index]
        assert all(
            tail in completed_nodes for tail, head in case.edges if head == index
        )
        assert set(node.references) <= committed_selectors
        selectors = (*node.outputs, *node.assignments)
        if selectors:
            updates = _produce_node_variables(case, graph, index)
            old_path_stream.variable_values.update(
                (selector, _format_selector_value(selector)) for selector in selectors
            )
            committed_selectors.update(selectors)
            assert_event_chunks(
                FrameVariablesUpdatedEvent(frame_id="root", variables=updates)
            )
        completed_nodes.add(index)
        for event, decision in _edge_decision_events(case, graph, index):
            old_path_stream.apply_edge_decision(*decision)
            assert_event_chunks(event)


@settings(derandomize=True, deadline=None)
@given(case=_gate_cases(NodeExecutionType.BRANCH))
@example(case=_GraphCase((_NodeCase(NodeExecutionType.RESPONSE),), ()))
@example(
    case=_GraphCase(
        (_NodeCase(NodeExecutionType.ROOT), _NodeCase(NodeExecutionType.RESPONSE)), ()
    )
)
@example(
    case=_GraphCase(
        (
            _NodeCase(NodeExecutionType.ROOT),
            _NodeCase(NodeExecutionType.RESPONSE),
            _NodeCase(NodeExecutionType.BRANCH),
        ),
        ((0, 2), (2, 1)),
        ((1, False), (1, True)),
    )
)
@example(
    case=_GraphCase(
        (
            _NodeCase(NodeExecutionType.ROOT),
            _NodeCase(NodeExecutionType.RESPONSE),
            _NodeCase(NodeExecutionType.BRANCH),
        ),
        ((0, 2), (2, 1)),
        ((1, True), (1, False)),
    )
)
def test_branch_gates_match_old_paths(case: _GraphCase) -> None:
    """Match legacy chunks after each edge decision in branch-gate cases."""
    _assert_response_chunks_match_old_paths(case)


@settings(derandomize=True, deadline=None)
@given(case=_gate_cases(NodeExecutionType.RESPONSE))
@example(
    case=_GraphCase(
        (
            _NodeCase(NodeExecutionType.ROOT),
            _NodeCase(NodeExecutionType.RESPONSE),
            _NodeCase(NodeExecutionType.RESPONSE),
        ),
        ((0, 2), (2, 1)),
        ((1, True), (0, True)),
    )
)
def test_previous_response_gates_match_old_paths(case: _GraphCase) -> None:
    """Match legacy chunks when earlier response nodes gate later responses."""
    _assert_response_chunks_match_old_paths(case)


@settings(derandomize=True, deadline=None)
@given(case=_variable_dependency_cases(assignments=False))
def test_node_output_dependencies_match_old_paths(case: _GraphCase) -> None:
    """Match legacy chunks as nonblocking producers commit referenced outputs."""
    _assert_response_chunks_match_old_paths(case)


@settings(derandomize=True, deadline=None)
@given(case=_variable_dependency_cases(assignments=True))
@example(
    case=_GraphCase(
        (
            _NodeCase(NodeExecutionType.ROOT),
            _NodeCase(
                NodeExecutionType.RESPONSE,
                references=(("conversation", "x"), ("conversation", "y")),
            ),
            _NodeCase(assignments=(("conversation", "x"),)),
            _NodeCase(assignments=(("conversation", "y"),)),
        ),
        ((0, 2), (2, 3), (3, 1)),
        ((0, True), (1, True), (2, True)),
        (0, 2, 3, 1),
    )
)
def test_assigner_dependencies_match_old_paths(case: _GraphCase) -> None:
    """Match legacy chunks as assigners overwrite referenced conversation variables."""
    _assert_response_chunks_match_old_paths(case)


@settings(derandomize=True, deadline=None)
@given(case=_distinct_response_dependency_cases())
@example(
    case=_GraphCase(
        (
            _NodeCase(NodeExecutionType.ROOT),
            _NodeCase(NodeExecutionType.RESPONSE, references=(("conversation", "y"),)),
            _NodeCase(NodeExecutionType.RESPONSE, references=(("conversation", "x"),)),
            _NodeCase(assignments=(("conversation", "x"),)),
            _NodeCase(assignments=(("conversation", "y"),)),
        ),
        ((0, 3), (3, 4), (4, 2), (4, 1)),
        ((0, True), (1, True), (2, True), (3, True)),
        (0, 3, 4, 2, 1),
    )
)
def test_responses_keep_distinct_assigner_dependencies(case: _GraphCase) -> None:
    """Match legacy chunks for replies sharing a path but using different writers."""
    _assert_response_chunks_match_old_paths(case)
