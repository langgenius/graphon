from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from typing import TYPE_CHECKING

from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.variables.segments import FileSegment, Segment
from graphon.variables.variables import Variable

from .runtime_state_protocol import ReadOnlyVariablePool
from .variable_pool import VariablePool

if TYPE_CHECKING:
    from .runtime_state import RuntimeState


class ReadOnlyVariablePoolWrapper(ReadOnlyVariablePool):
    """Provide defensive, read-only access to ``VariablePool``."""

    def __init__(self, variable_pool: VariablePool) -> None:
        self._variable_pool = variable_pool

    def get(self, selector: Sequence[str], /) -> Segment | None:
        """Return a copy of a variable value if present."""
        value = self._variable_pool.get(selector)
        return deepcopy(value) if value is not None else None

    def get_by_prefix(self, prefix: str, /) -> Mapping[str, object]:
        """Return a copy of all variables stored under the given prefix."""
        return self._variable_pool.get_by_prefix(prefix)

    def get_variable(self, selector: Sequence[str], /) -> Variable | None:
        return deepcopy(self._variable_pool.get_variable(selector))

    def get_file(self, selector: Sequence[str], /) -> FileSegment | None:
        return deepcopy(self._variable_pool.get_file(selector))

    def flatten(self, *, unprefixed_node_id: str | None = None) -> Mapping[str, object]:
        return self._variable_pool.flatten(unprefixed_node_id=unprefixed_node_id)

    def mutable_copy(self) -> VariablePool:
        """Create a private pool for several assignments within one node."""
        return self._variable_pool.model_copy(deep=True)


class ReadOnlyRuntimeStateWrapper:
    """Expose a defensive, read-only view of ``RuntimeState``."""

    def __init__(self, state: RuntimeState) -> None:
        self._state = state
        self._variable_pool_wrapper = ReadOnlyVariablePoolWrapper(state.variable_pool)

    @property
    def read_only_view(self) -> ReadOnlyRuntimeStateWrapper:
        """Return the owning frame's shared view."""
        return self._state.read_only_view

    @property
    def variable_pool(self) -> ReadOnlyVariablePoolWrapper:
        return self._variable_pool_wrapper

    @property
    def aborted(self) -> bool:
        return self._state.graph_execution.aborted

    @property
    def start_at(self) -> float:
        return self._state.start_at

    @property
    def total_tokens(self) -> int:
        return self._state.total_tokens

    @property
    def llm_usage(self) -> LLMUsage:
        return self._state.llm_usage.model_copy()

    @property
    def outputs(self) -> dict[str, object]:
        return deepcopy(self._state.outputs)

    @property
    def node_run_steps(self) -> int:
        return self._state.node_run_steps

    @property
    def ready_queue_size(self) -> int:
        return self._state.ready_queue.qsize()

    @property
    def exceptions_count(self) -> int:
        return self._state.graph_execution.exceptions_count

    def get_output(self, key: str, default: object = None) -> object:
        return self._state.get_output(key, default)

    def dumps(self) -> str:
        """Serialize quiescent state, enforcing the runtime's snapshot guard."""
        return self._state.dumps()
