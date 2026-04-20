from collections.abc import Mapping
from time import perf_counter
from typing import Any

from graphon.nodes.human_input.entities import (
    FormInput,
    HumanInputNodeData,
    ParagraphInput,
    StringSource,
)
from graphon.nodes.human_input.enums import ValueSourceType
from graphon.nodes.human_input.human_input_node import HumanInputNode
from graphon.nodes.runtime import (
    HumanInputFormStateProtocol,
    HumanInputNodeRuntimeProtocol,
)
from graphon.runtime.graph_runtime_state import GraphRuntimeState

from ...helpers import build_graph_init_params, build_variable_pool


class _RuntimeStub(HumanInputNodeRuntimeProtocol):
    def get_form(
        self,
        *,
        node_id: str,
    ) -> HumanInputFormStateProtocol | None:
        _ = node_id
        return None

    def create_form(
        self,
        *,
        node_id: str,
        node_data: HumanInputNodeData,
        rendered_content: str,
        resolved_default_values: Mapping[str, Any],
    ) -> HumanInputFormStateProtocol:
        _ = node_id, node_data, rendered_content, resolved_default_values
        msg = "create_form should not be called in resolve_default_values tests"
        raise AssertionError(msg)


def _build_node(
    *,
    inputs: list[FormInput],
    variables: tuple[tuple[tuple[str, ...], Any], ...] = (),
) -> HumanInputNode:
    runtime_state = GraphRuntimeState(
        variable_pool=build_variable_pool(variables=variables),
        start_at=perf_counter(),
    )
    return HumanInputNode(
        node_id="human-node",
        config=HumanInputNodeData(
            title="Collect Input",
            form_content="Profile",
            inputs=inputs,
        ),
        graph_init_params=build_graph_init_params(
            graph_config={"nodes": [], "edges": []},
        ),
        graph_runtime_state=runtime_state,
        runtime=_RuntimeStub(),
    )


class TestHumanInputNodeResolveDefaultValues:
    def test_resolve_default_values_skips_absent_constant_and_missing_defaults(
        self,
    ) -> None:
        node = _build_node(
            inputs=[
                ParagraphInput(output_variable_name="without_default"),
                ParagraphInput(
                    output_variable_name="constant_default",
                    default=StringSource(
                        type=ValueSourceType.CONSTANT,
                        value="Pinned text",
                    ),
                ),
                ParagraphInput(
                    output_variable_name="missing_default",
                    default=StringSource(
                        type=ValueSourceType.VARIABLE,
                        selector=("start", "missing"),
                    ),
                ),
                ParagraphInput(
                    output_variable_name="resolved_default",
                    default=StringSource(
                        type=ValueSourceType.VARIABLE,
                        selector=("start", "profile"),
                    ),
                ),
            ],
            variables=(
                (
                    ("start", "profile"),
                    {
                        "headline": "Graph runtime",
                        "tags": ["human-input", 3],
                    },
                ),
            ),
        )

        resolved = node.resolve_default_values()

        assert resolved == {
            "resolved_default": {
                "headline": "Graph runtime",
                "tags": ["human-input", 3],
            }
        }
