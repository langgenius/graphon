from collections.abc import Mapping, Sequence
from typing import Any, override

from graphon.enums import (
    BuiltinNodeTypes,
    NodeExecutionType,
    WorkflowNodeExecutionStatus,
)
from graphon.file.models import File
from graphon.node_events.base import NodeRunResult
from graphon.nodes.answer.entities import AnswerNodeData
from graphon.nodes.base.node import Node
from graphon.nodes.base.template import Template, TextSegment
from graphon.nodes.base.variable_template_parser import VariableTemplateParser
from graphon.variables.segments import ArrayFileSegment, FileSegment


class AnswerNode(Node[AnswerNodeData]):
    node_type = BuiltinNodeTypes.ANSWER
    execution_type = NodeExecutionType.RESPONSE

    @classmethod
    @override
    def version(cls) -> str:
        return "1"

    @override
    def _run(self) -> NodeRunResult:
        text_parts: list[str] = []
        files: list[File] = []
        for segment in Template.from_answer_template(self.node_data.answer).segments:
            if isinstance(segment, TextSegment):
                text_parts.append(segment.text)
                continue

            value = self.runtime_state.variable_pool.get(segment.selector)
            if value is None:
                continue
            text_parts.append(value.markdown)
            if isinstance(value, FileSegment):
                files.append(value.value)
            elif isinstance(value, ArrayFileSegment):
                files.extend(value.value)

        return NodeRunResult(
            status=WorkflowNodeExecutionStatus.SUCCEEDED,
            outputs={
                "answer": "".join(text_parts),
                "files": ArrayFileSegment(value=files),
            },
        )

    @classmethod
    @override
    def _extract_variable_selector_to_variable_mapping(
        cls,
        *,
        graph_config: Mapping[str, Any],
        node_id: str,
        node_data: AnswerNodeData,
    ) -> Mapping[str, Sequence[str]]:
        _ = graph_config  # Explicitly mark as unused
        variable_template_parser = VariableTemplateParser(template=node_data.answer)
        variable_selectors = variable_template_parser.extract_variable_selectors()

        variable_mapping = {}
        for variable_selector in variable_selectors:
            variable_mapping[node_id + "." + variable_selector.variable] = (
                variable_selector.value_selector
            )

        return variable_mapping

    def get_streaming_template(self) -> Template:
        """Get the template for streaming.

        Returns:
            Template instance for this Answer node

        """
        return Template.from_answer_template(self.node_data.answer)

    def get_streaming_text_selector(self) -> Sequence[str]:
        return [self.id, "answer"]
