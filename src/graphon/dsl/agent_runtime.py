from __future__ import annotations

from collections.abc import Generator, Mapping
from dataclasses import dataclass
from typing import Any

from graphon.nodes.agent.entities import AgentNodeData
from graphon.nodes.runtime import AgentNodeRuntimeProtocol
from graphon.nodes.tool_runtime_entities import ToolRuntimeMessage
from graphon.runtime.variable_pool import VariablePool

from ._provider import canonical_vendor
from .slim.agent import SlimActionInvoker, SlimAgentStrategyClient
from .slim.client import SlimClientConfig


@dataclass(slots=True)
class SlimAgentNodeRuntime(AgentNodeRuntimeProtocol):
    """Slim-backed runtime for Agent nodes.

    Args:
        config: Slim client configuration (mode, plugin folder, daemon
            address, etc.). Forwarded to each ``SlimAgentStrategyClient``.
        action_invoker: Optional dependency-injection seam matching
            ``SlimActionInvoker``. When set, slim subprocess invocation is
            bypassed entirely — used in tests.
    """

    config: SlimClientConfig
    action_invoker: SlimActionInvoker | None = None

    def invoke(
        self,
        *,
        node_id: str,
        node_data: AgentNodeData,
        agent_strategy_params: Mapping[str, Any],
        variable_pool: VariablePool | None,
    ) -> Generator[ToolRuntimeMessage, None, None]:
        """Invoke the strategy plugin and yield each decoded message.

        Yields:
            One ``ToolRuntimeMessage`` per slim chunk in the strategy's
            output stream.
        """
        # node_id / variable_pool are unused at this layer: the slim
        # action_invoker boundary is stateless and parameter resolution has
        # already happened in AgentNode._run before invoke() is called.
        _ = node_id, variable_pool

        provider_name = node_data.agent_strategy_provider_name
        client = SlimAgentStrategyClient(
            config=self.config,
            plugin_id=node_data.plugin_unique_identifier,
            agent_strategy_provider=canonical_vendor(provider_name) or provider_name,
            agent_strategy=node_data.agent_strategy_name,
            action_invoker=self.action_invoker,
        )
        yield from client.invoke(agent_strategy_params=agent_strategy_params)
