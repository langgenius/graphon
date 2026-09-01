from __future__ import annotations

import base64
import json
from collections.abc import Mapping, Sequence
from typing import Any

from graphon.runtime.init_params import InitParams
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.variables import Variable

_FILE_REFERENCE_PREFIX = "dify-file-ref:"


def build_file_reference(*, record_id: str, storage_key: str | None = None) -> str:
    payload = {"record_id": record_id}
    if storage_key is not None:
        payload["storage_key"] = storage_key

    encoded_payload = base64.urlsafe_b64encode(
        json.dumps(payload, separators=(",", ":")).encode(),
    ).decode()
    return f"{_FILE_REFERENCE_PREFIX}{encoded_payload}"


def build_init_params(
    *,
    workflow_id: str = "workflow",
    graph_config: Mapping[str, Any] | None = None,
    run_context: Mapping[str, Any] | None = None,
    call_depth: int = 0,
) -> InitParams:
    """Build the immutable inputs shared by nodes in one test execution.

    Optional mappings default to fresh empty dictionaries so callers can specify
    only the execution inputs relevant to a test without sharing mutable state.

    Args:
        workflow_id: Workflow identity exposed to constructed nodes.
        graph_config: Graph definition visible during node initialization.
        run_context: Immutable integration context visible to nodes.
        call_depth: Nested workflow call depth.

    Returns:
        A fully initialized ``InitParams`` value.

    """
    return InitParams(
        workflow_id=workflow_id,
        graph_config=graph_config or {},
        run_context=run_context or {},
        call_depth=call_depth,
    )


def build_variable_pool(
    *,
    system_variables: Sequence[Variable] = (),
    conversation_variables: Sequence[Variable] = (),
    variables: Sequence[tuple[Sequence[str], Any]] = (),
) -> VariablePool:
    variable_pool = VariablePool.from_bootstrap(
        system_variables=system_variables,
        conversation_variables=conversation_variables,
    )

    for selector, value in variables:
        variable_pool.add(selector, value)

    return variable_pool
