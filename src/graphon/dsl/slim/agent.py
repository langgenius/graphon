from __future__ import annotations

from collections.abc import Callable, Generator, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from graphon.dsl.slim.client import SlimClient, SlimClientConfig, SlimClientError
from graphon.dsl.tool_runtime import tool_runtime_message_from_payload
from graphon.nodes.tool.exc import ToolNodeError
from graphon.nodes.tool_runtime_entities import ToolRuntimeMessage

_SLIM_ACTION_INVOKE_AGENT_STRATEGY = "invoke_agent_strategy"


# Dify Plugin SDK defines `AgentInvokeMessage(InvokeMessage): pass` — wire format
# is identical to tool messages, so we share the DTO and decoder.
type AgentRuntimeMessage = ToolRuntimeMessage

type SlimActionInvoker = Callable[
    [str, str, Mapping[str, Any]],
    Iterable[Mapping[str, Any]],
]


class SlimAgentStrategyError(RuntimeError):
    """Raised when the slim ``invoke_agent_strategy`` action fails or returns
    a payload that cannot be decoded.

    The original error (subprocess failure, ``SlimClientError``, decode
    error, etc.) is preserved on ``__cause__`` via ``raise ... from error``
    so callers can introspect the failure source without parsing the
    message string.
    """


@dataclass(slots=True)
class SlimAgentStrategyClient:
    config: SlimClientConfig
    plugin_id: str
    agent_strategy_provider: str
    agent_strategy: str
    action_invoker: SlimActionInvoker | None = None
    _client: SlimClient | None = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.action_invoker is None:
            self._client = SlimClient(config=self.config)
        else:
            self._client = None

    def invoke(
        self,
        *,
        agent_strategy_params: Mapping[str, Any],
    ) -> Generator[AgentRuntimeMessage, None, None]:
        """Invoke the agent strategy and yield each decoded message chunk."""
        data = {
            "agent_strategy_provider": self.agent_strategy_provider,
            "agent_strategy": self.agent_strategy,
            "agent_strategy_params": dict(agent_strategy_params),
        }
        for payload in self._invoke_action(data):
            try:
                yield tool_runtime_message_from_payload(payload)
            except ToolNodeError as error:
                raise SlimAgentStrategyError(str(error)) from error

    def _invoke_action(
        self,
        data: Mapping[str, Any],
    ) -> Iterable[Mapping[str, Any]]:
        if self.action_invoker is not None:
            return self.action_invoker(
                self.plugin_id,
                _SLIM_ACTION_INVOKE_AGENT_STRATEGY,
                data,
            )
        return self._invoke_client(data)

    def _invoke_client(
        self,
        data: Mapping[str, Any],
    ) -> Generator[Mapping[str, Any], None, None]:
        if self._client is None:
            msg = "Slim client was not initialized."
            raise SlimAgentStrategyError(msg)
        try:
            for chunk in self._client.invoke_chunks(
                plugin_id=self.plugin_id,
                action=_SLIM_ACTION_INVOKE_AGENT_STRATEGY,
                data=data,
            ):
                if not isinstance(chunk, Mapping):
                    msg = f"Unexpected slim agent_strategy chunk: {chunk!r}"
                    raise SlimAgentStrategyError(msg)
                yield chunk
        except SlimClientError as error:
            raise SlimAgentStrategyError(str(error)) from error
