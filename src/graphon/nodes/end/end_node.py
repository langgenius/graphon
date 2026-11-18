from typing import override

from graphon.engine_events.node import ResponseRead
from graphon.enums import (
    BuiltinNodeTypes,
    NodeExecutionType,
    WorkflowNodeExecutionStatus,
)
from graphon.node_events.base import NodeRunResult
from graphon.nodes.base.node import Node
from graphon.nodes.base.response_reads import current_response_reads
from graphon.nodes.base.template import Template
from graphon.nodes.end.entities import EndNodeData
from graphon.variables.consts import SELECTORS_LENGTH


class EndNode(Node[EndNodeData]):
    node_type = BuiltinNodeTypes.END
    execution_type = NodeExecutionType.RESPONSE

    @classmethod
    @override
    def version(cls) -> str:
        return "1"

    @override
    def _run(self) -> NodeRunResult:
        """Run node - collect all outputs at once.

        This method runs after streaming is complete (if streaming was enabled).
        It collects all output variables and returns them.

        Returns:
            A succeeded `NodeRunResult` containing the collected outputs.

        """
        reads = current_response_reads()
        outputs = {}
        position = 0
        for variable_selector in self.node_data.outputs:
            selector = variable_selector.value_selector
            variable = self.runtime_state.variable_pool.get(selector)
            if (
                reads is not None
                and len(selector) == SELECTORS_LENGTH
                and selector[1] == "text"
            ):
                reads.append(
                    ResponseRead(
                        position=position,
                        selector=(selector[0], "text"),
                        value=variable.to_segment().model_copy(deep=True)
                        if variable is not None
                        else None,
                    )
                )
            value = variable.to_object() if variable is not None else None
            outputs[variable_selector.variable] = value
            # End streams one newline between fields and omits empty selectors.
            position += 2 if selector else 1

        return NodeRunResult(
            status=WorkflowNodeExecutionStatus.SUCCEEDED,
            inputs=outputs,
            outputs=outputs,
        )

    def get_streaming_template(self) -> Template:
        """Get the template for streaming.

        Returns:
            Template instance for this End node

        """
        outputs_config = [
            {"variable": output.variable, "value_selector": output.value_selector}
            for output in self.node_data.outputs
        ]
        return Template.from_end_outputs(outputs_config)
