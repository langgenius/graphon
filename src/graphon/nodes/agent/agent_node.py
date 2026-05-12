from collections.abc import Generator, Iterable, Mapping
from typing import Any, override

from graphon.entities.graph_init_params import GraphInitParams
from graphon.enums import BuiltinNodeTypes, WorkflowNodeExecutionStatus
from graphon.node_events.agent import AgentLogEvent
from graphon.node_events.base import NodeEventBase, NodeRunResult
from graphon.node_events.node import StreamChunkEvent, StreamCompletedEvent
from graphon.nodes.agent.entities import AgentNodeData, AgentParameterValue
from graphon.nodes.agent.exc import AgentNodeError
from graphon.nodes.base.node import Node
from graphon.nodes.runtime import AgentNodeRuntimeProtocol
from graphon.nodes.tool_runtime_entities import ToolRuntimeMessage
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.runtime.variable_pool import VariablePool


class AgentNode(Node[AgentNodeData]):
    """Workflow node that invokes a Dify ``agent_strategy`` plugin."""

    node_type = BuiltinNodeTypes.AGENT

    @override
    def __init__(
        self,
        node_id: str,
        data: AgentNodeData,
        *,
        graph_init_params: GraphInitParams,
        graph_runtime_state: GraphRuntimeState,
        runtime: AgentNodeRuntimeProtocol,
    ) -> None:
        super().__init__(
            node_id=node_id,
            data=data,
            graph_init_params=graph_init_params,
            graph_runtime_state=graph_runtime_state,
        )
        self._runtime = runtime

    @classmethod
    @override
    def version(cls) -> str:
        return "1"

    @override
    def _run(self) -> Generator[NodeEventBase, None, None]:
        variable_pool = self.graph_runtime_state.variable_pool

        try:
            resolved_params = _resolve_agent_parameters(
                self.node_data.agent_parameters,
                variable_pool=variable_pool,
            )
        except AgentNodeError as error:
            yield StreamCompletedEvent(
                node_run_result=NodeRunResult(
                    status=WorkflowNodeExecutionStatus.FAILED,
                    error=str(error),
                    error_type=type(error).__name__,
                ),
            )
            return

        text_buffer: list[str] = []
        json_outputs: list[Any] = []
        variable_outputs: dict[str, Any] = {}
        execution_id = self.ensure_execution_id()

        try:
            message_stream = self._runtime.invoke(
                node_id=self._node_id,
                node_data=self.node_data,
                agent_strategy_params=resolved_params,
                variable_pool=variable_pool,
            )
            for message in message_stream:
                yield from _dispatch_message(
                    message,
                    node_id=self._node_id,
                    execution_id=execution_id,
                    text_buffer=text_buffer,
                    json_outputs=json_outputs,
                    variable_outputs=variable_outputs,
                )
        except Exception as error:  # noqa: BLE001
            yield StreamCompletedEvent(
                node_run_result=NodeRunResult(
                    status=WorkflowNodeExecutionStatus.FAILED,
                    inputs=dict(resolved_params),
                    error=str(error),
                    error_type=type(error).__name__,
                ),
            )
            return

        outputs: dict[str, Any] = {"text": "".join(text_buffer)}
        if json_outputs:
            outputs["json"] = (
                json_outputs[-1] if len(json_outputs) == 1 else list(json_outputs)
            )
        outputs.update(variable_outputs)

        yield StreamCompletedEvent(
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.SUCCEEDED,
                inputs=dict(resolved_params),
                outputs=outputs,
            ),
        )


def _resolve_agent_parameters(
    parameters: Mapping[str, AgentParameterValue],
    *,
    variable_pool: VariablePool,
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, wrapper in parameters.items():
        match wrapper.type:
            case "constant":
                result[name] = wrapper.value
            case "mixed":
                template = str(wrapper.value) if wrapper.value is not None else ""
                result[name] = variable_pool.convert_template(template).text
            case "variable":
                if not isinstance(wrapper.value, list) or not all(
                    isinstance(part, str) for part in wrapper.value
                ):
                    msg = (
                        f"Variable agent parameter {name!r} must be a list of "
                        f"string segments."
                    )
                    raise AgentNodeError(msg)
                variable = variable_pool.get(wrapper.value)
                if variable is not None:
                    result[name] = variable.value
    return result


def _dispatch_message(
    message: ToolRuntimeMessage,
    *,
    node_id: str,
    execution_id: str,
    text_buffer: list[str],
    json_outputs: list[Any],
    variable_outputs: dict[str, Any],
) -> Iterable[NodeEventBase]:
    payload = message.message
    match message.type:
        case ToolRuntimeMessage.MessageType.TEXT | ToolRuntimeMessage.MessageType.LINK:
            if isinstance(payload, ToolRuntimeMessage.TextMessage):
                text_buffer.append(payload.text)
                yield StreamChunkEvent(
                    selector=[node_id, "text"],
                    chunk=payload.text,
                    is_final=False,
                )
        case ToolRuntimeMessage.MessageType.JSON:
            if (
                isinstance(payload, ToolRuntimeMessage.JsonMessage)
                and not payload.suppress_output
            ):
                json_outputs.append(payload.json_object)
        case ToolRuntimeMessage.MessageType.LOG:
            if isinstance(payload, ToolRuntimeMessage.LogMessage):
                yield AgentLogEvent(
                    message_id=payload.id,
                    label=payload.label,
                    node_execution_id=execution_id,
                    parent_id=payload.parent_id,
                    error=payload.error,
                    status=payload.status.value,
                    data=dict(payload.data),
                    metadata=dict(payload.metadata),
                    node_id=node_id,
                )
        case ToolRuntimeMessage.MessageType.VARIABLE:
            if isinstance(payload, ToolRuntimeMessage.VariableMessage):
                variable_outputs[payload.variable_name] = payload.variable_value
        case _:
            pass
