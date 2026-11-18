"""Separate small-DAG laws for traversal gates and executable variable dependencies."""

from collections import deque
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, replace
from typing import NamedTuple, cast, override

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


class _ResponseChunk(NamedTuple):
    node_id: str
    selector: tuple[str, ...]
    chunk: str
    is_final: bool


@dataclass(frozen=True, kw_only=True)
class _NodeCase:
    execution_type: NodeExecutionType = NodeExecutionType.EXECUTABLE
    outputs: tuple[_Selector, ...] = ()
    assignments: tuple[_Selector, ...] = ()
    references: tuple[_Selector, ...] = ()


@dataclass(frozen=True, kw_only=True)
class _EdgeCase:
    edge_id: str
    source: str
    target: str


@dataclass(frozen=True, kw_only=True)
class _EdgeDecision:
    edge_id: str
    was_taken: bool


@dataclass(frozen=True, kw_only=True)
class _GraphCase:
    """Graph inputs, with outgoing decisions grouped by execution_order when set."""

    nodes: Mapping[str, _NodeCase]
    edges: tuple[_EdgeCase, ...]
    edge_decisions: tuple[_EdgeDecision, ...] = ()
    execution_order: tuple[str, ...] = ()
    root_node_id: str = "start"


class _EdgeDecisionEvent(NamedTuple):
    event: EngineEvent
    decision: _EdgeDecision


class _NodeWithoutVariableBlocking(_TestNode):
    @override
    def blocks_variable_output(self, variable_selectors: set[tuple[str, ...]]) -> bool:
        # Match Node's default: owning an output does not make a node a blocker.
        return False


def _draw_edges(
    draw: st.DrawFn,
    order: list[str],
    max_size: int,
    *,
    first_edge_index: int = 0,
) -> list[_EdgeCase]:
    """Draw forward edges in the supplied order, allowing parallel edges."""
    node_id_strategy = st.sampled_from(order)
    ranks = {node_id: index for index, node_id in enumerate(order)}
    endpoints = [
        (source, target) if ranks[source] < ranks[target] else (target, source)
        for source, target in draw(
            st.lists(st.tuples(node_id_strategy, node_id_strategy), max_size=max_size)
        )
        if source != target
    ]
    return [
        _EdgeCase(edge_id=f"edge_{index}", source=source, target=target)
        for index, (source, target) in enumerate(endpoints, first_edge_index)
    ]


@st.composite
def _gate_cases(draw: st.DrawFn, gate_type: NodeExecutionType) -> _GraphCase:
    """Generate small DAGs with branch gates or preceding response gates.

    Returns:
        A 1-9-node DAG (3-9 for response gates) without variable references.
        Nodes or replies may be disconnected. Edge observations may be partial,
        repeated, reordered, or contradictory; they are not execution schedules.
    """
    node_count = draw(
        st.integers(
            min_value=3 if gate_type == NodeExecutionType.RESPONSE else 1, max_value=9
        )
    )
    node_ids = [
        "start",
        *(["answer"] if node_count > 1 else []),
        *[f"node_{index}" for index in range(2, node_count)],
    ]
    order = ["start", *node_ids[2:], *node_ids[1:2]]
    edges = _draw_edges(draw, order, 18)
    nodes = dict.fromkeys(node_ids, _NodeCase())
    nodes["start"] = _NodeCase(execution_type=NodeExecutionType.ROOT)
    nodes["answer" if node_count > 1 else "start"] = _NodeCase(
        execution_type=NodeExecutionType.RESPONSE
    )
    gates = (
        draw(
            st.sets(
                st.sampled_from(node_ids[2:]),
                min_size=1 if gate_type == NodeExecutionType.RESPONSE else 0,
            )
        )
        if node_count > 2
        else set()
    )
    for node_id in gates:
        nodes[node_id] = _NodeCase(execution_type=gate_type)
    decisions = (
        draw(
            st.lists(
                st.builds(
                    _EdgeDecision,
                    edge_id=st.sampled_from([edge.edge_id for edge in edges]),
                    was_taken=st.booleans(),
                ),
                max_size=2 * len(edges) + 6,
            )
        )
        if edges
        else []
    )
    return _GraphCase(nodes=nodes, edges=tuple(edges), edge_decisions=tuple(decisions))


def _draw_rooted_edges(
    draw: st.DrawFn, order: list[str], required_parents: Mapping[str, str]
) -> list[_EdgeCase]:
    """Draw a rooted DAG, preserving a sole predecessor where one is required."""
    edges = [
        _EdgeCase(
            edge_id=f"edge_{position - 1}",
            source=required_parents[node_id]
            if node_id in required_parents
            else draw(st.sampled_from(order[:position])),
            target=node_id,
        )
        for position, node_id in enumerate(order[1:], 1)
    ]
    edges.extend(
        edge
        for edge in _draw_edges(
            draw, order, 18 - len(edges), first_edge_index=len(edges)
        )
        if edge.target not in required_parents
        or required_parents[edge.target] == edge.source
    )
    return edges


def _find_dominators(order: list[str], edges: list[_EdgeCase]) -> dict[str, set[str]]:
    dominators_by_node = {order[0]: {order[0]}}
    for node_id in order[1:]:
        dominators_by_node[node_id] = {node_id} | set.intersection(
            *(
                dominators_by_node[edge.source]
                for edge in edges
                if edge.target == node_id
            )
        )
    return dominators_by_node


def _draw_execution_case(
    draw: st.DrawFn, nodes: Mapping[str, _NodeCase], edges: list[_EdgeCase]
) -> _GraphCase:
    """Draw a topological execution with every edge taken and optional duplicates."""
    pending = set(nodes)
    order = []
    while pending:
        ready = sorted(
            node_id
            for node_id in pending
            if all(
                edge.source not in pending for edge in edges if edge.target == node_id
            )
        )
        node_id = draw(st.sampled_from(ready))
        pending.remove(node_id)
        order.append(node_id)
    edge_ids = tuple(edge.edge_id for edge in edges)
    edge_order = draw(st.permutations(edge_ids))
    repeats = draw(st.lists(st.sampled_from(edge_ids), max_size=3))
    return _GraphCase(
        nodes=nodes,
        edges=tuple(edges),
        edge_decisions=tuple(
            _EdgeDecision(edge_id=edge_id, was_taken=True)
            for edge_id in (*edge_order, *repeats)
        ),
        execution_order=tuple(order),
    )


@st.composite
def _variable_dependency_cases(draw: st.DrawFn, *, assignments: bool) -> _GraphCase:
    """Generate 3-9-node rooted DAGs with one reply and required upstream values.

    Returns:
        A rooted DAG referencing 1-3 values from producers that dominate its
        reply; unrelated producers may also exist. Values are ordinary node
        outputs or real assigner writes. Commits precede outgoing taken events
        in topological node order, with duplicate observations allowed.
    """
    node_count = draw(st.integers(min_value=3, max_value=9))
    producers = [f"producer_{index}" for index in range(node_count - 2)]
    order = ["start", *producers, "answer"]
    parent = draw(st.sampled_from(producers))
    edges = _draw_rooted_edges(draw, order, {"answer": parent})
    dominators = _find_dominators(order, edges)["answer"] - {"start", "answer"}
    nodes = dict.fromkeys(order, _NodeCase())
    nodes["start"] = _NodeCase(execution_type=NodeExecutionType.ROOT)
    for node_id in producers:
        selector_count = draw(
            st.integers(min_value=1 if node_id == parent else 0, max_value=3)
        )
        selectors = tuple(
            ("conversation", f"{node_id}_value{field}")
            if assignments
            else (node_id, f"value{field}")
            for field in range(selector_count)
        )
        nodes[node_id] = (
            _NodeCase(assignments=selectors)
            if assignments
            else _NodeCase(outputs=selectors)
        )
    available_selectors = [
        selector
        for node_id in sorted(dominators)
        for selector in (*nodes[node_id].outputs, *nodes[node_id].assignments)
    ]
    references = draw(
        st.lists(
            st.sampled_from(available_selectors),
            min_size=1,
            max_size=min(3, len(available_selectors)),
            unique=True,
        )
    )
    nodes["answer"] = _NodeCase(
        execution_type=NodeExecutionType.RESPONSE, references=tuple(references)
    )
    return _draw_execution_case(draw, nodes, edges)


@st.composite
def _distinct_response_dependency_cases(draw: st.DrawFn) -> _GraphCase:
    """Generate 5-9-node DAGs with two replies sharing a chain of two writers.

    Returns:
        A shared-path case where the early reply reads only the first writer,
        and the late reply reads the second and possibly earlier values.
        The late reply may follow the early response node. References are
        upstream and all edge decisions are taken in topological node order.
    """
    node_count = draw(st.integers(min_value=5, max_value=9))
    extra_nodes = [f"extra_{index}" for index in range(node_count - 5)]
    order = [
        "start",
        "first_writer",
        *extra_nodes,
        "second_writer",
        "early_reply",
        "late_reply",
    ]
    last_parent = draw(st.sampled_from(["second_writer", "early_reply"]))
    edges = _draw_rooted_edges(
        draw,
        order,
        {
            "second_writer": "first_writer",
            "early_reply": "second_writer",
            "late_reply": last_parent,
        },
    )
    dominators = _find_dominators(order, edges)
    nodes = dict.fromkeys(order, _NodeCase())
    nodes["start"] = _NodeCase(execution_type=NodeExecutionType.ROOT)
    for node_id in ["first_writer", "second_writer", *extra_nodes]:
        selector_count = draw(
            st.integers(
                min_value=1 if node_id in {"first_writer", "second_writer"} else 0,
                max_value=2,
            )
        )
        nodes[node_id] = _NodeCase(
            assignments=tuple(
                ("conversation", f"{node_id}_value{field}")
                for field in range(selector_count)
            )
        )
    for response, writer in [
        ("early_reply", "first_writer"),
        ("late_reply", "second_writer"),
    ]:
        required_selector = draw(st.sampled_from(nodes[writer].assignments))
        available_selectors = (
            list(nodes[writer].assignments)
            if response == "early_reply"
            else [
                selector
                for node_id in sorted(dominators[response] - {response})
                for selector in nodes[node_id].assignments
            ]
        )
        extra_references = draw(
            st.lists(st.sampled_from(available_selectors), max_size=2, unique=True)
        )
        nodes[response] = _NodeCase(
            execution_type=NodeExecutionType.RESPONSE,
            references=tuple(dict.fromkeys([required_selector, *extra_references])),
        )
    return _draw_execution_case(draw, nodes, edges)


def _enumerate_old_blocking_paths(case: _GraphCase, response: str) -> list[list[str]]:
    references = set(case.nodes[response].references)
    blockers = {
        node_id
        for node_id, node in case.nodes.items()
        if node.execution_type in {NodeExecutionType.BRANCH, NodeExecutionType.RESPONSE}
        or references.intersection(node.assignments)
    }

    def enumerate_paths(node_id: str, path: list[_EdgeCase]) -> list[list[str]]:
        if node_id == response:
            return [[edge.edge_id for edge in path if edge.source in blockers]]
        return [
            complete
            for edge in case.edges
            if edge.source == node_id
            for complete in enumerate_paths(edge.target, [*path, edge])
        ]

    return enumerate_paths(case.root_node_id, [])


def _format_selector_value(selector: _Selector) -> str:
    return ":".join(selector)


def _build_initial_variables(case: _GraphCase) -> tuple[Variable, ...]:
    return tuple(
        StringVariable(
            name=selector[1],
            selector=selector,
            value=f"old:{_format_selector_value(selector)}",
        )
        for node in case.nodes.values()
        for selector in node.assignments
    )


def _build_graph(case: _GraphCase) -> _TestGraph:
    pool = VariablePool()
    for variable in _build_initial_variables(case):
        pool.add(variable.selector, variable)
    runtime_state = RuntimeState(workflow_id="workflow", variable_pool=pool, start_at=0)
    nodes: dict[str, NodeProtocol] = {}
    for node_id, node in case.nodes.items():
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
            edge.edge_id: _TestEdge(edge.edge_id, edge.source, edge.target)
            for edge in case.edges
        },
        root_node_id=case.root_node_id,
    )


class _OldPathResponseStream:
    def __init__(self, case: _GraphCase) -> None:
        self.case = case
        self.blocking_paths = {
            node_id: _enumerate_old_blocking_paths(case, node_id)
            for node_id, node in case.nodes.items()
            if node.execution_type == NodeExecutionType.RESPONSE
        }
        self.variable_values = {
            cast(_Selector, tuple(variable.selector)): str(variable.value)
            for variable in _build_initial_variables(case)
        }
        self.started_responses: set[str] = set()
        self.waiting_responses: deque[str] = deque()
        self.segment_positions: dict[str, int] = dict.fromkeys(self.blocking_paths, 0)
        self.skipped_edge_ids: set[str] = set()

    def apply_edge_decision(self, decision: _EdgeDecision) -> None:
        if not decision.was_taken:
            self.skipped_edge_ids.add(decision.edge_id)
        for response, paths in self.blocking_paths.items():
            if decision.was_taken:
                for path in paths:
                    if decision.edge_id in path:
                        path.remove(decision.edge_id)
            else:
                self.blocking_paths[response] = [
                    path for path in paths if decision.edge_id not in path
                ]

    def _source_was_skipped(self, selector: _Selector) -> bool:
        if selector[0] not in self.case.nodes:
            return False
        # Walk backwards independently of the filter's forward skip scan.
        pending = [selector[0]]
        visited: set[str] = set()
        while pending:
            node_id = pending.pop()
            if node_id == self.case.root_node_id:
                return False
            if node_id in visited:
                continue
            visited.add(node_id)
            pending.extend(
                edge.source
                for edge in self.case.edges
                if edge.target == node_id and edge.edge_id not in self.skipped_edge_ids
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
            references = self.case.nodes[response].references
            position = self.segment_positions[response]
            if position == 0:
                chunks.append(
                    _ResponseChunk(
                        node_id=response,
                        selector=(response, "answer"),
                        chunk=f"[{response}]",
                        is_final=False,
                    )
                )
            elif position <= len(references):
                selector = references[position - 1]
                if self._source_was_skipped(selector):
                    self.segment_positions[response] += 1
                    continue
                if selector not in self.variable_values:
                    return chunks
                source = selector[0] if selector[0] in self.case.nodes else response
                chunks.append(
                    _ResponseChunk(
                        node_id=source,
                        selector=selector,
                        chunk=self.variable_values[selector],
                        is_final=False,
                    )
                )
            else:
                chunks.append(
                    _ResponseChunk(
                        node_id=response,
                        selector=(response, "answer"),
                        chunk=";",
                        is_final=True,
                    )
                )
                self.waiting_responses.popleft()
            self.segment_positions[response] += 1
        return chunks


def _edge_decision_events(
    case: _GraphCase, graph: _TestGraph, node_id: str | None = None
) -> Iterator[_EdgeDecisionEvent]:
    for decision in case.edge_decisions:
        edge = graph.edges[decision.edge_id]
        if node_id is not None and edge.tail != node_id:
            continue
        event = (
            _edge_taken(edge_id=edge.id, source_id=edge.tail, answer_id=edge.head)
            if decision.was_taken
            else GraphEdgeSkippedEvent(
                frame_id="root",
                edge_id=edge.id,
                source_node_id=edge.tail,
                target_node_id=edge.head,
            )
        )
        yield _EdgeDecisionEvent(event=event, decision=decision)


def _produce_node_variables(
    case: _GraphCase, graph: _TestGraph, node_id: str
) -> tuple[Variable, ...]:
    node = case.nodes[node_id]
    if not node.assignments:
        return tuple(
            StringVariable(
                name=selector[1],
                selector=selector,
                value=_format_selector_value(selector),
            )
            for selector in node.outputs
        )
    assigner = cast(VariableAssignerNode, graph.nodes[node_id])
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
            _ResponseChunk(
                node_id=item.node_id,
                selector=tuple(item.selector),
                chunk=item.chunk,
                is_final=item.is_final,
            )
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
        for edge_event in _edge_decision_events(case, graph):
            old_path_stream.apply_edge_decision(edge_event.decision)
            assert_event_chunks(edge_event.event)
        return
    completed_nodes: set[str] = set()
    committed_selectors: set[_Selector] = set()
    for node_id in case.execution_order:
        node = case.nodes[node_id]
        assert all(
            edge.source in completed_nodes
            for edge in case.edges
            if edge.target == node_id
        )
        assert set(node.references) <= committed_selectors
        selectors = (*node.outputs, *node.assignments)
        if selectors:
            updates = _produce_node_variables(case, graph, node_id)
            old_path_stream.variable_values.update(
                (selector, _format_selector_value(selector)) for selector in selectors
            )
            committed_selectors.update(selectors)
            assert_event_chunks(
                FrameVariablesUpdatedEvent(frame_id="root", variables=updates)
            )
        completed_nodes.add(node_id)
        for edge_event in _edge_decision_events(case, graph, node_id):
            old_path_stream.apply_edge_decision(edge_event.decision)
            assert_event_chunks(edge_event.event)


@settings(derandomize=True, deadline=None)
@given(case=_gate_cases(NodeExecutionType.BRANCH))
@example(
    case=_GraphCase(
        nodes={"start": _NodeCase(execution_type=NodeExecutionType.RESPONSE)}, edges=()
    )
)
@example(
    case=_GraphCase(
        nodes={
            "start": _NodeCase(execution_type=NodeExecutionType.ROOT),
            "answer": _NodeCase(execution_type=NodeExecutionType.RESPONSE),
        },
        edges=(),
    )
)
@example(
    case=_GraphCase(
        nodes={
            "start": _NodeCase(execution_type=NodeExecutionType.ROOT),
            "answer": _NodeCase(execution_type=NodeExecutionType.RESPONSE),
            "branch": _NodeCase(execution_type=NodeExecutionType.BRANCH),
        },
        edges=(
            _EdgeCase(edge_id="start_to_branch", source="start", target="branch"),
            _EdgeCase(edge_id="branch_to_answer", source="branch", target="answer"),
        ),
        edge_decisions=(
            _EdgeDecision(edge_id="branch_to_answer", was_taken=False),
            _EdgeDecision(edge_id="branch_to_answer", was_taken=True),
        ),
    )
)
@example(
    case=_GraphCase(
        nodes={
            "start": _NodeCase(execution_type=NodeExecutionType.ROOT),
            "answer": _NodeCase(execution_type=NodeExecutionType.RESPONSE),
            "branch": _NodeCase(execution_type=NodeExecutionType.BRANCH),
        },
        edges=(
            _EdgeCase(edge_id="start_to_branch", source="start", target="branch"),
            _EdgeCase(edge_id="branch_to_answer", source="branch", target="answer"),
        ),
        edge_decisions=(
            _EdgeDecision(edge_id="branch_to_answer", was_taken=True),
            _EdgeDecision(edge_id="branch_to_answer", was_taken=False),
        ),
    )
)
def test_branch_gates_match_old_paths(case: _GraphCase) -> None:
    """Open the reply once any root path has all its branch gates satisfied.

    Ordinary edges need no taken event. If a branch edge's first decision is
    skip, that path is rejected permanently; a first take survives later skips.
    One surviving path is sufficient, and repeated events must not replay output.
    """
    _assert_response_chunks_match_old_paths(case)


@settings(derandomize=True, deadline=None)
@given(case=_gate_cases(NodeExecutionType.RESPONSE))
@example(
    case=_GraphCase(
        nodes={
            "start": _NodeCase(execution_type=NodeExecutionType.ROOT),
            "first_reply": _NodeCase(execution_type=NodeExecutionType.RESPONSE),
            "last_reply": _NodeCase(execution_type=NodeExecutionType.RESPONSE),
        },
        edges=(
            _EdgeCase(edge_id="start_to_first", source="start", target="first_reply"),
            _EdgeCase(
                edge_id="first_to_last", source="first_reply", target="last_reply"
            ),
        ),
        edge_decisions=(
            _EdgeDecision(edge_id="first_to_last", was_taken=True),
            _EdgeDecision(edge_id="start_to_first", was_taken=True),
        ),
    )
)
def test_previous_response_gates_match_old_paths(case: _GraphCase) -> None:
    """A reply's outgoing edges gate later replies, not that reply itself.

    The earlier reply can emit before its outgoing edges are taken. Each later
    reply needs one root path whose preceding response edges were first observed
    as taken; an edge first observed as skipped rejects only paths through it.
    Repeated observations must not replay output.
    """
    _assert_response_chunks_match_old_paths(case)


@settings(derandomize=True, deadline=None)
@given(case=_variable_dependency_cases(assignments=False))
def test_node_output_dependencies_match_old_paths(case: _GraphCase) -> None:
    """Ordinary executable producers do not gate replies merely by owning outputs.

    Their variable-blocking hooks return false. The literal prefix emits before
    outputs exist; variable occurrences wait for commits and emit in template
    order. The closing literal and final marker wait for all required values.
    """
    _assert_response_chunks_match_old_paths(case)


@settings(derandomize=True, deadline=None)
@given(case=_variable_dependency_cases(assignments=True))
@example(
    case=_GraphCase(
        nodes={
            "start": _NodeCase(execution_type=NodeExecutionType.ROOT),
            "answer": _NodeCase(
                execution_type=NodeExecutionType.RESPONSE,
                references=(("conversation", "x"), ("conversation", "y")),
            ),
            "first_writer": _NodeCase(assignments=(("conversation", "x"),)),
            "second_writer": _NodeCase(assignments=(("conversation", "y"),)),
        },
        edges=(
            _EdgeCase(edge_id="start_to_first", source="start", target="first_writer"),
            _EdgeCase(
                edge_id="first_to_second", source="first_writer", target="second_writer"
            ),
            _EdgeCase(
                edge_id="second_to_answer", source="second_writer", target="answer"
            ),
        ),
        edge_decisions=(
            _EdgeDecision(edge_id="start_to_first", was_taken=True),
            _EdgeDecision(edge_id="first_to_second", was_taken=True),
            _EdgeDecision(edge_id="second_to_answer", was_taken=True),
        ),
        execution_order=("start", "first_writer", "second_writer", "answer"),
    )
)
def test_assigner_dependencies_match_old_paths(case: _GraphCase) -> None:
    """Gate each path on assigners that write selectors referenced by the reply.

    The writer, not the selector's namespace, supplies the gate. A value commit
    alone does not open its outgoing edge; a taken event does. Unreferenced
    writers add no gates. Once a full path opens, the reply reads committed
    updates in template order rather than initial conversation values.
    """
    _assert_response_chunks_match_old_paths(case)


@settings(derandomize=True, deadline=None)
@given(case=_distinct_response_dependency_cases())
@example(
    case=_GraphCase(
        nodes={
            "start": _NodeCase(execution_type=NodeExecutionType.ROOT),
            "early_reply": _NodeCase(
                execution_type=NodeExecutionType.RESPONSE,
                references=(("conversation", "x"),),
            ),
            "late_reply": _NodeCase(
                execution_type=NodeExecutionType.RESPONSE,
                references=(("conversation", "y"),),
            ),
            "first_writer": _NodeCase(assignments=(("conversation", "x"),)),
            "second_writer": _NodeCase(assignments=(("conversation", "y"),)),
        },
        edges=(
            _EdgeCase(edge_id="start_to_first", source="start", target="first_writer"),
            _EdgeCase(
                edge_id="first_to_second", source="first_writer", target="second_writer"
            ),
            _EdgeCase(
                edge_id="second_to_early", source="second_writer", target="early_reply"
            ),
            _EdgeCase(
                edge_id="second_to_late", source="second_writer", target="late_reply"
            ),
        ),
        edge_decisions=(
            _EdgeDecision(edge_id="start_to_first", was_taken=True),
            _EdgeDecision(edge_id="first_to_second", was_taken=True),
            _EdgeDecision(edge_id="second_to_early", was_taken=True),
            _EdgeDecision(edge_id="second_to_late", was_taken=True),
        ),
        execution_order=(
            "start",
            "first_writer",
            "second_writer",
            "early_reply",
            "late_reply",
        ),
    )
)
def test_responses_keep_distinct_assigner_dependencies(case: _GraphCase) -> None:
    """The early reply opens before the second writer; the late reply does not.

    Taking first_writer -> second_writer opens early_reply, which reads only
    first_writer. late_reply still waits for a taken edge leaving second_writer
    and, when present, a preceding response edge. Sharing the path must not
    merge the replies' different selector blockers.
    """
    _assert_response_chunks_match_old_paths(case)
