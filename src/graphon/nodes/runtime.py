from __future__ import annotations

from abc import abstractmethod
from collections.abc import Generator, Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.nodes.tool_runtime_entities import (
    ToolRuntimeHandle,
    ToolRuntimeMessage,
    ToolRuntimeParameter,
)

if TYPE_CHECKING:
    from graphon.nodes.human_input.entities import HumanInputNodeData
    from graphon.nodes.human_input.enums import HumanInputFormStatus
    from graphon.nodes.tool.entities import ToolNodeData
    from graphon.runtime.variable_pool import VariablePool


class ToolNodeRuntimeProtocol(Protocol):
    """Workflow-layer adapter owned by `core.workflow` and consumed by `graphon`.

    The graph package depends only on these DTOs and lets the workflow layer
    translate between graph-owned abstractions and `core.tools` internals.
    """

    @abstractmethod
    def get_runtime(
        self,
        *,
        node_id: str,
        node_data: ToolNodeData,
        variable_pool: VariablePool | None,
        node_execution_id: str | None = None,
    ) -> ToolRuntimeHandle: ...

    @abstractmethod
    def get_runtime_parameters(
        self,
        *,
        tool_runtime: ToolRuntimeHandle,
    ) -> Sequence[ToolRuntimeParameter]: ...

    @abstractmethod
    def invoke(
        self,
        *,
        tool_runtime: ToolRuntimeHandle,
        tool_parameters: Mapping[str, Any],
        workflow_call_depth: int,
        provider_name: str,
    ) -> Generator[ToolRuntimeMessage, None, None]: ...

    @abstractmethod
    def get_usage(
        self,
        *,
        tool_runtime: ToolRuntimeHandle,
    ) -> LLMUsage: ...

    @abstractmethod
    def build_file_reference(self, *, mapping: Mapping[str, Any]) -> Any: ...


@runtime_checkable
class HumanInputNodeRuntimeProtocol(Protocol):
    """Workflow-layer adapter for human-input runtime persistence and delivery."""

    @abstractmethod
    def get_form(
        self,
        *,
        node_id: str,
    ) -> HumanInputFormStateProtocol | None: ...

    @abstractmethod
    def create_form(
        self,
        *,
        node_id: str,
        node_data: HumanInputNodeData,
        rendered_content: str,
        resolved_default_values: Mapping[str, Any],
    ) -> HumanInputFormStateProtocol: ...


@runtime_checkable
class HumanInputFormRepositoryBindableRuntimeProtocol(Protocol):
    """Optional capability for runtimes that require explicit repository binding."""

    @abstractmethod
    def with_form_repository(
        self,
        form_repository: object,
    ) -> HumanInputNodeRuntimeProtocol: ...


_HumanInputRuntimeLike = (
    HumanInputNodeRuntimeProtocol | HumanInputFormRepositoryBindableRuntimeProtocol
)


def _normalize_human_input_runtime(
    runtime: _HumanInputRuntimeLike,
    *,
    form_repository: object | None = None,
) -> HumanInputNodeRuntimeProtocol:
    """Return a runnable human-input runtime, binding a repository when needed."""
    if form_repository is not None and isinstance(
        runtime, HumanInputFormRepositoryBindableRuntimeProtocol
    ):
        bound_runtime = runtime.with_form_repository(form_repository)
        if not isinstance(bound_runtime, HumanInputNodeRuntimeProtocol):
            msg = "with_form_repository() must return a HumanInput runtime"
            raise TypeError(msg)
        return bound_runtime

    if isinstance(runtime, HumanInputNodeRuntimeProtocol):
        return runtime

    if isinstance(runtime, HumanInputFormRepositoryBindableRuntimeProtocol):
        msg = (
            "form_repository is required when runtime only supports "
            "with_form_repository()"
        )
        raise TypeError(msg)

    msg = (
        "runtime must implement HumanInputNodeRuntimeProtocol or "
        "HumanInputFormRepositoryBindableRuntimeProtocol"
    )
    raise TypeError(msg)


class HumanInputFormStateProtocol(Protocol):
    @property
    @abstractmethod
    def id(self) -> str: ...

    @property
    @abstractmethod
    def rendered_content(self) -> str: ...

    @property
    @abstractmethod
    def selected_action_id(self) -> str | None: ...

    @property
    @abstractmethod
    def submitted_data(self) -> Mapping[str, Any] | None: ...

    @property
    @abstractmethod
    def submitted(self) -> bool: ...

    @property
    @abstractmethod
    def status(self) -> HumanInputFormStatus: ...

    @property
    @abstractmethod
    def expiration_time(self) -> datetime: ...
