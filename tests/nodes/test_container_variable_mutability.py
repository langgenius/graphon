import time
from unittest.mock import patch

from graphon.nodes.iteration.entities import IterationNodeData
from graphon.nodes.iteration.iteration_node import IterationNode
from graphon.nodes.loop.entities import LoopNodeData, LoopVariableData
from graphon.nodes.loop.loop_node import LoopNode
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.types import SegmentType

from ..helpers import build_graph_init_params


def test_loop_initializes_writable_loop_variables() -> None:
    runtime_state = GraphRuntimeState(
        variable_pool=VariablePool.empty(),
        start_at=time.perf_counter(),
    )
    node = LoopNode(
        node_id="loop",
        graph_init_params=build_graph_init_params(
            graph_config={"nodes": [], "edges": []}
        ),
        graph_runtime_state=runtime_state,
        data=LoopNodeData(
            title="Loop",
            start_node_id="loop-start",
            loop_count=1,
            break_conditions=[],
            logical_operator="and",
            loop_variables=[
                LoopVariableData(
                    label="index",
                    var_type=SegmentType.NUMBER,
                    value_type="constant",
                    value=0,
                ),
            ],
        ),
    )

    selectors = node._initialize_loop_variables(inputs={})  # noqa: SLF001
    variable = runtime_state.variable_pool.get_variable(selectors["index"])

    assert variable is not None
    assert variable.writable is True


def test_iteration_child_pool_marks_item_and_index_writable() -> None:
    runtime_state = GraphRuntimeState(
        variable_pool=VariablePool.empty(),
        start_at=time.perf_counter(),
    )
    node = IterationNode(
        node_id="iteration",
        graph_init_params=build_graph_init_params(
            graph_config={"nodes": [], "edges": []}
        ),
        graph_runtime_state=runtime_state,
        data=IterationNodeData(
            title="Iteration",
            start_node_id="iteration-start",
            iterator_selector=["source", "items"],
            output_selector=["target", "output"],
        ),
    )

    with patch.object(
        runtime_state, "create_child_engine", return_value=object()
    ) as create_child_engine:
        node._create_graph_engine(index=2, item="hello")  # noqa: SLF001

    child_pool = create_child_engine.call_args.kwargs["variable_pool"]
    index_variable = child_pool.get_variable(("iteration", "index"))
    item_variable = child_pool.get_variable(("iteration", "item"))

    assert index_variable is not None
    assert index_variable.writable is True
    assert item_variable is not None
    assert item_variable.writable is True
