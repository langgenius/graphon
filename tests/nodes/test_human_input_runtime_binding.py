from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from time import perf_counter
from typing import Any, cast

import pytest

from graphon.nodes.human_input.entities import HumanInputNodeData
from graphon.nodes.human_input.enums import HumanInputFormStatus
from graphon.nodes.human_input.human_input_node import HumanInputNode
from graphon.nodes.runtime import (
    HumanInputFormRepositoryBindableRuntimeProtocol,
    HumanInputFormStateProtocol,
    HumanInputNodeRuntimeProtocol,
)
from graphon.runtime.graph_runtime_state import GraphRuntimeState

from ..helpers import build_graph_init_params, build_variable_pool


class _StubHumanInputRuntime:
    def get_form(self, *, node_id: str) -> HumanInputFormStateProtocol | None:
        _ = node_id
        msg = "not used in this test"
        raise AssertionError(msg)

    def create_form(
        self,
        *,
        node_id: str,
        node_data: HumanInputNodeData,
        rendered_content: str,
        resolved_default_values: Mapping[str, Any],
    ) -> HumanInputFormStateProtocol:
        _ = (node_id, node_data, rendered_content, resolved_default_values)
        msg = "not used in this test"
        raise AssertionError(msg)


class _StubFormState:
    def __init__(self) -> None:
        self._id = "form-1"
        self._rendered_content = "Rendered form"
        self._expiration_time = datetime.now(UTC).replace(tzinfo=None)

    @property
    def id(self) -> str:
        return self._id

    @property
    def rendered_content(self) -> str:
        return self._rendered_content

    @property
    def selected_action_id(self) -> str | None:
        return None

    @property
    def submitted_data(self) -> Mapping[str, Any] | None:
        return None

    @property
    def submitted(self) -> bool:
        return False

    @property
    def status(self) -> HumanInputFormStatus:
        return HumanInputFormStatus.WAITING

    @property
    def expiration_time(self) -> datetime:
        return self._expiration_time


class _RunnableHumanInputRuntime(_StubHumanInputRuntime):
    def __init__(self) -> None:
        self.get_form_calls: list[str] = []
        self.create_form_calls: list[str] = []

    def get_form(self, *, node_id: str) -> HumanInputFormStateProtocol | None:
        self.get_form_calls.append(node_id)
        return None

    def create_form(
        self,
        *,
        node_id: str,
        node_data: HumanInputNodeData,
        rendered_content: str,
        resolved_default_values: Mapping[str, Any],
    ) -> HumanInputFormStateProtocol:
        _ = (node_data, rendered_content, resolved_default_values)
        self.create_form_calls.append(node_id)
        return _StubFormState()


class _BindableHumanInputRuntime:
    def __init__(
        self,
        *,
        bound_runtime: HumanInputNodeRuntimeProtocol | None = None,
    ) -> None:
        self.bound_runtime = bound_runtime or _StubHumanInputRuntime()
        self.bound_repositories: list[object] = []

    def with_form_repository(
        self,
        form_repository: object,
    ) -> HumanInputNodeRuntimeProtocol:
        self.bound_repositories.append(form_repository)
        return self.bound_runtime


class _RebindableHumanInputRuntime(_StubHumanInputRuntime):
    def __init__(self, rebound_runtime: HumanInputNodeRuntimeProtocol) -> None:
        self.rebound_runtime = rebound_runtime
        self.bound_repositories: list[object] = []

    def with_form_repository(
        self,
        form_repository: object,
    ) -> HumanInputNodeRuntimeProtocol:
        self.bound_repositories.append(form_repository)
        return self.rebound_runtime


class _InvalidBindableHumanInputRuntime:
    def with_form_repository(
        self,
        form_repository: object,
    ) -> HumanInputNodeRuntimeProtocol:
        _ = form_repository
        return cast(HumanInputNodeRuntimeProtocol, object())


def _build_human_input_node(
    *,
    runtime: (
        HumanInputNodeRuntimeProtocol | HumanInputFormRepositoryBindableRuntimeProtocol
    ),
    form_repository: object | None = None,
) -> HumanInputNode:
    return HumanInputNode(
        node_id="human-input-node",
        data=HumanInputNodeData(title="Human Input"),
        graph_init_params=build_graph_init_params(
            graph_config={"nodes": [], "edges": []},
        ),
        graph_runtime_state=GraphRuntimeState(
            variable_pool=build_variable_pool(),
            start_at=perf_counter(),
        ),
        runtime=runtime,
        form_repository=form_repository,
    )


def test_human_input_node_accepts_ready_runtime_without_repository() -> None:
    runtime = _RunnableHumanInputRuntime()

    node = _build_human_input_node(runtime=runtime)

    list(node.run())

    assert runtime.get_form_calls == ["human-input-node"]
    assert runtime.create_form_calls == ["human-input-node"]


def test_human_input_node_requires_repository_for_bindable_runtime() -> None:
    with pytest.raises(
        TypeError,
        match="form_repository is required when runtime only supports",
    ):
        _build_human_input_node(runtime=_BindableHumanInputRuntime())


def test_human_input_node_rejects_invalid_bound_runtime() -> None:
    with pytest.raises(
        TypeError,
        match="with_form_repository\\(\\) must return a HumanInput runtime",
    ):
        _build_human_input_node(
            runtime=_InvalidBindableHumanInputRuntime(),
            form_repository=object(),
        )


def test_human_input_node_prefers_explicit_runtime_binding() -> None:
    repository = object()
    rebound_runtime = _RunnableHumanInputRuntime()
    runtime = _RebindableHumanInputRuntime(rebound_runtime)

    node = _build_human_input_node(runtime=runtime, form_repository=repository)

    list(node.run())

    assert rebound_runtime.get_form_calls == ["human-input-node"]
    assert rebound_runtime.create_form_calls == ["human-input-node"]
    assert runtime.bound_repositories == [repository]
