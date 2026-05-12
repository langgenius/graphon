from __future__ import annotations

import time
from pathlib import Path
from typing import cast

from graphon.dsl.agent_runtime import SlimAgentNodeRuntime
from graphon.dsl.entities import DslCredentials, DslSlimSettings
from graphon.dsl.node_factory import SlimDslNodeFactory
from graphon.entities.graph_config import NodeConfigDict
from graphon.entities.graph_init_params import GraphInitParams
from graphon.nodes.agent import AgentNode
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.runtime.variable_pool import VariablePool


def _factory(tmp_path: Path) -> SlimDslNodeFactory:
    return SlimDslNodeFactory(
        graph_config={"nodes": [], "edges": []},
        graph_init_params=GraphInitParams(
            workflow_id="wf",
            graph_config={"nodes": [], "edges": []},
            run_context={},
            call_depth=0,
        ),
        graph_runtime_state=GraphRuntimeState(
            variable_pool=VariablePool.from_bootstrap(),
            start_at=time.time(),
        ),
        credentials=DslCredentials(
            slim=DslSlimSettings(mode="local", plugin_folder=str(tmp_path)),
        ),
        dependencies=[],
    )


def test_create_node_routes_agent_type_to_agent_node(tmp_path: Path) -> None:
    factory = _factory(tmp_path)
    node_config = {
        "id": "agent-1",
        "data": {
            "type": "agent",
            "title": "Agent",
            "agent_strategy_provider_name": "langgenius/agent/agent",
            "agent_strategy_name": "function_calling",
            "plugin_unique_identifier": "langgenius/agent:0.0.36@hash",
            "agent_parameters": {
                "instruction": {"type": "constant", "value": "be helpful"},
            },
            "tool_node_version": "2",
        },
    }

    node = factory.create_node(cast("NodeConfigDict", node_config))

    assert isinstance(node, AgentNode)
    assert node.node_data.agent_strategy_name == "function_calling"
    assert node.node_data.plugin_unique_identifier == "langgenius/agent:0.0.36@hash"


def test_create_node_injects_slim_agent_node_runtime(tmp_path: Path) -> None:
    """The factory's default agent runtime is the slim-backed adapter — node
    instantiation must succeed without any extra wiring at the call site.
    """
    factory = _factory(tmp_path)
    node_config = {
        "id": "agent-1",
        "data": {
            "type": "agent",
            "title": "Agent",
            "agent_strategy_provider_name": "langgenius/agent/agent",
            "agent_strategy_name": "ReAct",
            "plugin_unique_identifier": "langgenius/agent:0.0.36@hash",
            "agent_parameters": {},
        },
    }

    node = factory.create_node(cast("NodeConfigDict", node_config))
    assert isinstance(node, AgentNode)
    # Access the private slot to confirm the wired runtime is the slim adapter.
    # Acceptable in tests; mirrors the pattern used elsewhere when verifying
    # factory wiring.
    assert isinstance(node._runtime, SlimAgentNodeRuntime)
