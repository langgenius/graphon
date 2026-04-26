import logging
from collections.abc import Generator, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import suppress
from datetime import UTC, datetime
from threading import Lock
from typing import TYPE_CHECKING, Any, Literal, NewType, assert_never, override

from typing_extensions import TypeIs

from graphon.entities.graph_config import NodeConfigDictAdapter
from graphon.entities.graph_init_params import GraphInitParams
from graphon.enums import (
    BuiltinNodeTypes,
    NodeExecutionType,
    WorkflowNodeExecutionMetadataKey,
    WorkflowNodeExecutionStatus,
)
from graphon.graph_events.base import GraphEngineEvent, GraphNodeEventBase
from graphon.graph_events.graph import (
    GraphRunAbortedEvent,
    GraphRunFailedEvent,
    GraphRunPartialSucceededEvent,
    GraphRunSucceededEvent,
)
from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.node_events.base import (
    NodeEventBase,
    NodeRunResult,
)
from graphon.node_events.iteration import (
    IterationFailedEvent,
    IterationNextEvent,
    IterationStartedEvent,
    IterationSucceededEvent,
)
from graphon.node_events.node import StreamCompletedEvent
from graphon.nodes.base.node import Node
from graphon.nodes.base.usage_tracking_mixin import LLMUsageTrackingMixin
from graphon.nodes.iteration.entities import ErrorHandleMode, IterationNodeData
from graphon.runtime.graph_runtime_state import ChildGraphNotFoundError
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.segments import ArrayAnySegment, ArraySegment, NoneSegment
from graphon.variables.variables import IntegerVariable

from .exc import (
    ChildGraphAbortedError,
    InvalidIteratorValueError,
    IterationGraphNotFoundError,
    IterationIndexNotFoundError,
    IterationNodeError,
    IteratorVariableNotFoundError,
    StartNodeIdNotFoundError,
)

if TYPE_CHECKING:
    from graphon.graph_engine.graph_engine import GraphEngine

logger = logging.getLogger(__name__)
_DEFAULT_CHILD_ABORT_REASON = "child graph aborted"

EmptyArraySegment = NewType("EmptyArraySegment", ArraySegment)
type _ParallelIterationResult = tuple[
    float, list[GraphNodeEventBase], object | None, LLMUsage
]
type _ParallelIterationFuture = Future[_ParallelIterationResult]


class IterationNode(LLMUsageTrackingMixin, Node[IterationNodeData]):
    """Iteration Node."""

    node_type = BuiltinNodeTypes.ITERATION
    execution_type = NodeExecutionType.CONTAINER

    @classmethod
    @override
    def get_default_config(
        cls,
        filters: Mapping[str, object] | None = None,
    ) -> Mapping[str, object]:
        _ = filters
        return {
            "type": "iteration",
            "config": {
                "is_parallel": False,
                "parallel_nums": 10,
                "error_handle_mode": ErrorHandleMode.TERMINATED,
                "flatten_output": True,
            },
        }

    @classmethod
    @override
    def version(cls) -> str:
        return "1"

    @override
    def _run(self) -> Generator[GraphNodeEventBase | NodeEventBase, None, None]:
        variable = self._get_iterator_variable()

        if self._is_empty_iteration(variable):
            yield from self._handle_empty_iteration(variable)
            return

        iterator_list_value = self._validate_and_get_iterator_list(variable)
        inputs = {"iterator_selector": iterator_list_value}

        self._validate_start_node()

        started_at = datetime.now(UTC).replace(tzinfo=None)
        iter_run_map: dict[str, float] = {}
        outputs: list[object] = []
        usage_accumulator = [LLMUsage.empty_usage()]

        yield IterationStartedEvent(
            start_at=started_at,
            inputs=inputs,
            metadata={"iteration_length": len(iterator_list_value)},
        )

        try:
            yield from self._execute_iterations(
                iterator_list_value=iterator_list_value,
                outputs=outputs,
                iter_run_map=iter_run_map,
                usage_accumulator=usage_accumulator,
            )

            self._accumulate_usage(usage_accumulator[0])
            yield from self._handle_iteration_success(
                started_at=started_at,
                inputs=inputs,
                outputs=outputs,
                iterator_list_value=iterator_list_value,
                iter_run_map=iter_run_map,
                usage=usage_accumulator[0],
            )
        except IterationNodeError as e:
            self._accumulate_usage(usage_accumulator[0])
            yield from self._handle_iteration_failure(
                started_at=started_at,
                inputs=inputs,
                outputs=outputs,
                iterator_list_value=iterator_list_value,
                iter_run_map=iter_run_map,
                usage=usage_accumulator[0],
                error=e,
            )

    def _get_iterator_variable(self) -> ArraySegment | NoneSegment:
        variable = self.graph_runtime_state.variable_pool.get(
            self.node_data.iterator_selector,
        )

        if not variable:
            msg = f"iterator variable {self.node_data.iterator_selector} not found"
            raise IteratorVariableNotFoundError(msg)

        if not isinstance(variable, ArraySegment) and not isinstance(
            variable,
            NoneSegment,
        ):
            msg = f"invalid iterator value: {variable}, please provide a list."
            raise InvalidIteratorValueError(msg)

        return variable

    def _is_empty_iteration(
        self,
        variable: ArraySegment | NoneSegment,
    ) -> TypeIs[NoneSegment | EmptyArraySegment]:
        return isinstance(variable, NoneSegment) or len(variable.value) == 0

    def _handle_empty_iteration(
        self,
        variable: ArraySegment | NoneSegment,
    ) -> Generator[NodeEventBase, None, None]:
        # Try our best to preserve the type information.
        if isinstance(variable, ArraySegment):
            output = variable.model_copy(update={"value": []})
        else:
            output = ArrayAnySegment(value=[])

        yield StreamCompletedEvent(
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.SUCCEEDED,
                outputs={"output": output},
            ),
        )

    def _validate_and_get_iterator_list(
        self,
        variable: ArraySegment,
    ) -> Sequence[object]:
        iterator_list_value = variable.to_object()

        if not isinstance(iterator_list_value, list):
            msg = (
                f"Invalid iterator value: {iterator_list_value}, please provide a list."
            )
            raise InvalidIteratorValueError(msg)

        return iterator_list_value

    def _validate_start_node(self) -> None:
        if not self.node_data.start_node_id:
            msg = f"field start_node_id in iteration {self._node_id} not found"
            raise StartNodeIdNotFoundError(msg)

    def _execute_iterations(
        self,
        iterator_list_value: Sequence[object],
        outputs: list[object],
        iter_run_map: dict[str, float],
        usage_accumulator: list[LLMUsage],
    ) -> Generator[GraphNodeEventBase | NodeEventBase, None, None]:
        if self.node_data.is_parallel:
            # Parallel mode execution
            yield from self._execute_parallel_iterations(
                iterator_list_value=iterator_list_value,
                outputs=outputs,
                iter_run_map=iter_run_map,
                usage_accumulator=usage_accumulator,
            )
        else:
            # Sequential mode execution
            for index, item in enumerate(iterator_list_value):
                iter_start_at = datetime.now(UTC).replace(tzinfo=None)
                yield IterationNextEvent(index=index)

                graph_engine = self._create_graph_engine(index, item)

                # Run the iteration
                try:
                    yield from self._run_single_iter(
                        variable_pool=graph_engine.graph_runtime_state.variable_pool,
                        outputs=outputs,
                        graph_engine=graph_engine,
                    )
                finally:
                    self._merge_graph_engine_usage(
                        usage_accumulator=usage_accumulator,
                        graph_engine=graph_engine,
                    )
                iter_run_map[str(index)] = (
                    datetime.now(UTC).replace(tzinfo=None) - iter_start_at
                ).total_seconds()

    def _execute_parallel_iterations(
        self,
        iterator_list_value: Sequence[object],
        outputs: list[object],
        iter_run_map: dict[str, float],
        usage_accumulator: list[LLMUsage],
    ) -> Generator[GraphNodeEventBase | NodeEventBase, None, None]:
        outputs.extend([None] * len(iterator_list_value))
        max_workers = min(self.node_data.parallel_nums, len(iterator_list_value))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            started_child_engines: dict[int, GraphEngine] = {}
            started_child_engines_lock = Lock()
            merged_usage_indexes: set[int] = set()
            future_to_index: dict[_ParallelIterationFuture, int] = {}
            for index, item in enumerate(iterator_list_value):
                yield IterationNextEvent(index=index)
                future = self._submit_parallel_iteration_task(
                    executor=executor,
                    index=index,
                    item=item,
                    started_child_engines=started_child_engines,
                    started_child_engines_lock=started_child_engines_lock,
                )
                future_to_index[future] = index
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    yield from self._handle_parallel_iteration_success(
                        index=index,
                        result=future.result(),
                        outputs=outputs,
                        iter_run_map=iter_run_map,
                        usage_accumulator=usage_accumulator,
                        merged_usage_indexes=merged_usage_indexes,
                    )
                except Exception as e:
                    action = self._handle_parallel_iteration_exception(
                        error=e,
                        index=index,
                        future=future,
                        future_to_index=future_to_index,
                        outputs=outputs,
                        started_child_engines=started_child_engines,
                        usage_accumulator=usage_accumulator,
                        merged_usage_indexes=merged_usage_indexes,
                    )
                    if action == "reraise":
                        raise
                    if action == "terminate":
                        raise IterationNodeError(str(e)) from e

        self._finalize_parallel_outputs(outputs)

    def _submit_parallel_iteration_task(
        self,
        *,
        executor: ThreadPoolExecutor,
        index: int,
        item: object,
        started_child_engines: dict[int, "GraphEngine"],
        started_child_engines_lock: Lock,
    ) -> _ParallelIterationFuture:
        return executor.submit(
            self._execute_tracked_iteration_parallel,
            index=index,
            item=item,
            started_child_engines=started_child_engines,
            started_child_engines_lock=started_child_engines_lock,
        )

    def _handle_parallel_iteration_success(
        self,
        *,
        index: int,
        result: _ParallelIterationResult,
        outputs: list[object],
        iter_run_map: dict[str, float],
        usage_accumulator: list[LLMUsage],
        merged_usage_indexes: set[int],
    ) -> Generator[GraphNodeEventBase, None, None]:
        iteration_duration, events, output_value, iteration_usage = result
        outputs[index] = output_value
        yield from events
        iter_run_map[str(index)] = iteration_duration
        usage_accumulator[0] = self._merge_usage(
            usage_accumulator[0],
            iteration_usage,
        )
        merged_usage_indexes.add(index)

    def _handle_parallel_iteration_exception(
        self,
        *,
        error: Exception,
        index: int,
        future: _ParallelIterationFuture,
        future_to_index: Mapping[_ParallelIterationFuture, int],
        outputs: list[object],
        started_child_engines: Mapping[int, "GraphEngine"],
        usage_accumulator: list[LLMUsage],
        merged_usage_indexes: set[int],
    ) -> Literal["handled", "reraise", "terminate"]:
        self._merge_parallel_iteration_usage_if_needed(
            index=index,
            started_child_engines=started_child_engines,
            usage_accumulator=usage_accumulator,
            merged_usage_indexes=merged_usage_indexes,
        )
        if isinstance(error, ChildGraphAbortedError):
            self._abort_parallel_siblings(
                future_to_index=future_to_index,
                current_future=future,
                started_child_engines=started_child_engines,
                reason=str(error) or _DEFAULT_CHILD_ABORT_REASON,
            )
            self._drain_parallel_siblings(
                future_to_index=future_to_index,
                current_future=future,
                started_child_engines=started_child_engines,
                usage_accumulator=usage_accumulator,
                merged_usage_indexes=merged_usage_indexes,
            )
            return "reraise"

        error_handle_mode = self.node_data.error_handle_mode
        match error_handle_mode:
            case ErrorHandleMode.TERMINATED:
                for pending_future in future_to_index:
                    if pending_future != future:
                        pending_future.cancel()
                return "terminate"
            case (
                ErrorHandleMode.CONTINUE_ON_ERROR
                | ErrorHandleMode.REMOVE_ABNORMAL_OUTPUT
            ):
                outputs[index] = None
                return "handled"
            case _:
                assert_never(error_handle_mode)

    def _merge_parallel_iteration_usage_if_needed(
        self,
        *,
        index: int,
        started_child_engines: Mapping[int, "GraphEngine"],
        usage_accumulator: list[LLMUsage],
        merged_usage_indexes: set[int],
    ) -> None:
        if index in merged_usage_indexes:
            return
        self._merge_graph_engine_usage(
            usage_accumulator=usage_accumulator,
            graph_engine=started_child_engines.get(index),
        )
        merged_usage_indexes.add(index)

    def _finalize_parallel_outputs(self, outputs: list[object]) -> None:
        if self.node_data.error_handle_mode == ErrorHandleMode.REMOVE_ABNORMAL_OUTPUT:
            outputs[:] = [output for output in outputs if output is not None]

    @staticmethod
    def _merge_graph_engine_usage(
        *,
        usage_accumulator: list[LLMUsage],
        graph_engine: "GraphEngine | None",
    ) -> None:
        if graph_engine is None:
            return
        usage_accumulator[0] = IterationNode._merge_usage(
            usage_accumulator[0],
            graph_engine.graph_runtime_state.llm_usage,
        )

    def _abort_parallel_siblings(
        self,
        *,
        future_to_index: Mapping[
            Future[tuple[float, list[GraphNodeEventBase], object | None, LLMUsage]],
            int,
        ],
        current_future: Future[
            tuple[float, list[GraphNodeEventBase], object | None, LLMUsage]
        ],
        started_child_engines: Mapping[int, "GraphEngine"],
        reason: str,
    ) -> None:
        for future, index in future_to_index.items():
            if future == current_future:
                continue

            graph_engine = started_child_engines.get(index)
            if graph_engine is not None:
                graph_engine.request_abort(reason)

            future.cancel()

    def _drain_parallel_siblings(
        self,
        *,
        future_to_index: Mapping[
            Future[tuple[float, list[GraphNodeEventBase], object | None, LLMUsage]],
            int,
        ],
        current_future: Future[
            tuple[float, list[GraphNodeEventBase], object | None, LLMUsage]
        ],
        started_child_engines: Mapping[int, "GraphEngine"],
        usage_accumulator: list[LLMUsage],
        merged_usage_indexes: set[int],
    ) -> None:
        for future, index in future_to_index.items():
            if future == current_future:
                continue
            if future.cancelled():
                continue

            with suppress(Exception):
                future.result()

            if index in merged_usage_indexes:
                continue

            self._merge_graph_engine_usage(
                usage_accumulator=usage_accumulator,
                graph_engine=started_child_engines.get(index),
            )
            merged_usage_indexes.add(index)

    def _execute_tracked_iteration_parallel(
        self,
        *,
        index: int,
        item: object,
        started_child_engines: dict[int, "GraphEngine"],
        started_child_engines_lock: Lock,
    ) -> tuple[float, list[GraphNodeEventBase], object | None, LLMUsage]:
        graph_engine = self._create_graph_engine(index, item)
        with started_child_engines_lock:
            started_child_engines[index] = graph_engine

        return self._execute_parallel_iteration_with_graph_engine(
            graph_engine=graph_engine,
        )

    def _execute_single_iteration_parallel(
        self,
        index: int,
        item: object,
    ) -> tuple[float, list[GraphNodeEventBase], object | None, LLMUsage]:
        """Execute a single iteration in parallel mode and return results."""
        graph_engine = self._create_graph_engine(index, item)
        return self._execute_parallel_iteration_with_graph_engine(
            graph_engine=graph_engine,
        )

    def _execute_parallel_iteration_with_graph_engine(
        self,
        *,
        graph_engine: "GraphEngine",
    ) -> tuple[float, list[GraphNodeEventBase], object | None, LLMUsage]:
        """Execute a prepared child engine in parallel mode and return results."""
        iter_start_at = datetime.now(UTC).replace(tzinfo=None)
        outputs_temp: list[object] = []

        # Collect events instead of yielding them directly
        events = list(
            self._run_single_iter(
                variable_pool=graph_engine.graph_runtime_state.variable_pool,
                outputs=outputs_temp,
                graph_engine=graph_engine,
            ),
        )

        # Get the output value from the temporary outputs list
        output_value = outputs_temp[0] if outputs_temp else None
        iteration_duration = (
            datetime.now(UTC).replace(tzinfo=None) - iter_start_at
        ).total_seconds()

        return (
            iteration_duration,
            events,
            output_value,
            graph_engine.graph_runtime_state.llm_usage,
        )

    def _handle_iteration_success(
        self,
        started_at: datetime,
        inputs: dict[str, Sequence[object]],
        outputs: list[object],
        iterator_list_value: Sequence[object],
        iter_run_map: dict[str, float],
        *,
        usage: LLMUsage,
    ) -> Generator[NodeEventBase, None, None]:
        # Flatten the list of lists if all outputs are lists
        flattened_outputs = self._flatten_outputs_if_needed(outputs)

        yield IterationSucceededEvent(
            start_at=started_at,
            inputs=inputs,
            outputs={"output": flattened_outputs},
            steps=len(iterator_list_value),
            metadata={
                WorkflowNodeExecutionMetadataKey.TOTAL_TOKENS: usage.total_tokens,
                WorkflowNodeExecutionMetadataKey.TOTAL_PRICE: usage.total_price,
                WorkflowNodeExecutionMetadataKey.CURRENCY: usage.currency,
                WorkflowNodeExecutionMetadataKey.ITERATION_DURATION_MAP: iter_run_map,
            },
        )

        # Yield final success event
        yield StreamCompletedEvent(
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.SUCCEEDED,
                outputs={"output": flattened_outputs},
                metadata={
                    WorkflowNodeExecutionMetadataKey.TOTAL_TOKENS: usage.total_tokens,
                    WorkflowNodeExecutionMetadataKey.TOTAL_PRICE: usage.total_price,
                    WorkflowNodeExecutionMetadataKey.CURRENCY: usage.currency,
                },
                llm_usage=usage,
            ),
        )

    def _flatten_outputs_if_needed(self, outputs: list[object]) -> list[object]:
        """Flatten the outputs list if all elements are lists.
        This maintains backward compatibility with version 1.8.1 behavior.

        If flatten_output is False, returns outputs as-is (nested structure).
        If flatten_output is True (default), flattens the list if all
        elements are lists.

        Returns:
            Either the original outputs or a flattened list, depending on settings.

        """
        # If flatten_output is disabled, return outputs as-is
        if not self.node_data.flatten_output:
            return outputs

        if not outputs:
            return outputs

        # Check if all non-None outputs are lists
        non_none_outputs: list[object] = [
            output for output in outputs if output is not None
        ]
        if not non_none_outputs:
            return outputs

        if all(isinstance(output, list) for output in non_none_outputs):
            # Flatten the list of lists
            flattened: list[object] = []
            for output in outputs:
                if isinstance(output, list):
                    flattened.extend(output)
                elif output is not None:
                    # This shouldn't happen based on our check, but handle it gracefully
                    flattened.append(output)
            return flattened

        return outputs

    def _handle_iteration_failure(
        self,
        started_at: datetime,
        inputs: dict[str, Sequence[object]],
        outputs: list[object],
        iterator_list_value: Sequence[object],
        iter_run_map: dict[str, float],
        *,
        usage: LLMUsage,
        error: IterationNodeError,
    ) -> Generator[NodeEventBase, None, None]:
        # Flatten the list of lists if all outputs are lists (even in failure case)
        flattened_outputs = self._flatten_outputs_if_needed(outputs)

        yield IterationFailedEvent(
            start_at=started_at,
            inputs=inputs,
            outputs={"output": flattened_outputs},
            steps=len(iterator_list_value),
            metadata={
                WorkflowNodeExecutionMetadataKey.TOTAL_TOKENS: usage.total_tokens,
                WorkflowNodeExecutionMetadataKey.TOTAL_PRICE: usage.total_price,
                WorkflowNodeExecutionMetadataKey.CURRENCY: usage.currency,
                WorkflowNodeExecutionMetadataKey.ITERATION_DURATION_MAP: iter_run_map,
            },
            error=str(error),
        )
        yield StreamCompletedEvent(
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.FAILED,
                error=str(error),
                metadata={
                    WorkflowNodeExecutionMetadataKey.TOTAL_TOKENS: usage.total_tokens,
                    WorkflowNodeExecutionMetadataKey.TOTAL_PRICE: usage.total_price,
                    WorkflowNodeExecutionMetadataKey.CURRENCY: usage.currency,
                },
                llm_usage=usage,
            ),
        )

    @classmethod
    @override
    def _extract_variable_selector_to_variable_mapping(
        cls,
        *,
        graph_config: Mapping[str, Any],
        node_id: str,
        node_data: IterationNodeData,
    ) -> Mapping[str, Sequence[str]]:
        variable_mapping: dict[str, Sequence[str]] = {
            f"{node_id}.input_selector": node_data.iterator_selector,
        }
        iteration_node_ids = set()

        # Find all nodes that belong to this loop
        nodes = graph_config.get("nodes", [])
        for node in nodes:
            node_config_data = node.get("data", {})
            if node_config_data.get("iteration_id") == node_id:
                in_iteration_node_id = node.get("id")
                if in_iteration_node_id:
                    iteration_node_ids.add(in_iteration_node_id)

        # Get node configs from graph_config instead of non-existent mapping.
        node_configs = {
            node["id"]: node for node in graph_config.get("nodes", []) if "id" in node
        }
        for sub_node_id, sub_node_config in node_configs.items():
            if sub_node_config.get("data", {}).get("iteration_id") != node_id:
                continue

            # variable selector to variable mapping
            try:
                typed_sub_node_config = NodeConfigDictAdapter.validate_python(
                    sub_node_config,
                )
                node_type = typed_sub_node_config["data"].type
                node_mapping = Node.get_node_type_classes_mapping()
                if node_type not in node_mapping:
                    continue
                node_version = str(typed_sub_node_config["data"].version)
                node_cls = node_mapping[node_type][node_version]

                sub_node_variable_mapping = (
                    node_cls.extract_variable_selector_to_variable_mapping(
                        graph_config=graph_config,
                        config=typed_sub_node_config,
                    )
                )
            except NotImplementedError:
                sub_node_variable_mapping = {}

            # remove iteration variables
            sub_node_variable_mapping = {
                sub_node_id + "." + key: value
                for key, value in sub_node_variable_mapping.items()
                if value[0] != node_id
            }

            variable_mapping.update(sub_node_variable_mapping)

        # remove variable out from iteration
        return {
            key: value
            for key, value in variable_mapping.items()
            if value[0] not in iteration_node_ids
        }

    def _append_iteration_info_to_event(
        self,
        event: GraphNodeEventBase,
        iter_run_index: int,
    ) -> None:
        event.in_iteration_id = self._node_id
        iter_metadata = {
            WorkflowNodeExecutionMetadataKey.ITERATION_ID: self._node_id,
            WorkflowNodeExecutionMetadataKey.ITERATION_INDEX: iter_run_index,
        }

        current_metadata = event.node_run_result.metadata
        if WorkflowNodeExecutionMetadataKey.ITERATION_ID not in current_metadata:
            event.node_run_result.metadata = {**current_metadata, **iter_metadata}

    def _run_single_iter(
        self,
        *,
        variable_pool: VariablePool,
        outputs: list[object],
        graph_engine: "GraphEngine",
    ) -> Generator[GraphNodeEventBase, None, None]:
        current_index = self._get_current_iteration_index(variable_pool)
        for event in graph_engine.run():
            event_to_yield, stop_iteration = self._process_single_iteration_event(
                event=event,
                current_index=current_index,
                variable_pool=variable_pool,
                outputs=outputs,
            )
            if event_to_yield is not None:
                yield event_to_yield
            if stop_iteration:
                break

        return

    def run_single_iter(
        self,
        *,
        variable_pool: VariablePool,
        outputs: list[object],
        graph_engine: "GraphEngine",
    ) -> Generator[GraphNodeEventBase, None, None]:
        """Run a single iteration with explicit collaborators."""
        yield from self._run_single_iter(
            variable_pool=variable_pool,
            outputs=outputs,
            graph_engine=graph_engine,
        )

    def _get_current_iteration_index(self, variable_pool: VariablePool) -> int:
        index_variable = variable_pool.get([self._node_id, "index"])
        if not isinstance(index_variable, IntegerVariable):
            msg = f"iteration {self._node_id} current index not found"
            raise IterationIndexNotFoundError(msg)
        return index_variable.value

    def _process_single_iteration_event(
        self,
        *,
        event: GraphEngineEvent,
        current_index: int,
        variable_pool: VariablePool,
        outputs: list[object],
    ) -> tuple[GraphNodeEventBase | None, bool]:
        match event:
            case GraphNodeEventBase(node_type=BuiltinNodeTypes.ITERATION_START):
                return None, False
            case GraphNodeEventBase():
                self._append_iteration_info_to_event(
                    event=event,
                    iter_run_index=current_index,
                )
                return event, False
            case GraphRunSucceededEvent() | GraphRunPartialSucceededEvent():
                result = variable_pool.get(self.node_data.output_selector)
                outputs.append(None if result is None else result.to_object())
                return None, True
            case GraphRunAbortedEvent(reason=reason):
                raise ChildGraphAbortedError(reason or _DEFAULT_CHILD_ABORT_REASON)
            case GraphRunFailedEvent(error=error):
                return self._handle_failed_single_iteration(
                    error=error,
                    outputs=outputs,
                )
            case _:
                return None, False

    def _handle_failed_single_iteration(
        self,
        *,
        error: str,
        outputs: list[object],
    ) -> tuple[GraphNodeEventBase | None, bool]:
        error_handle_mode = self.node_data.error_handle_mode
        match error_handle_mode:
            case ErrorHandleMode.TERMINATED:
                raise IterationNodeError(error)
            case ErrorHandleMode.CONTINUE_ON_ERROR:
                outputs.append(None)
                return None, True
            case ErrorHandleMode.REMOVE_ABNORMAL_OUTPUT:
                return None, True
            case _:
                assert_never(error_handle_mode)

    def _create_graph_engine(self, index: int, item: object) -> Any:
        # Create GraphInitParams for child graph execution.
        graph_init_params = GraphInitParams(
            workflow_id=self.workflow_id,
            graph_config=self.graph_config,
            run_context=self.run_context,
            call_depth=self.workflow_call_depth,
        )
        # Create a deep copy of the variable pool for each iteration
        variable_pool_copy = self.graph_runtime_state.variable_pool.model_copy(
            deep=True,
        )

        # append iteration variable (item, index) to variable pool
        variable_pool_copy.add([self._node_id, "index"], index)
        variable_pool_copy.add([self._node_id, "item"], item)
        root_node_id = self.node_data.start_node_id
        if root_node_id is None:
            msg = f"field start_node_id in iteration {self._node_id} not found"
            raise StartNodeIdNotFoundError(msg)

        try:
            return self.graph_runtime_state.create_child_engine(
                workflow_id=self.workflow_id,
                graph_init_params=graph_init_params,
                root_node_id=root_node_id,
                variable_pool=variable_pool_copy,
            )
        except ChildGraphNotFoundError as exc:
            msg = "iteration graph not found"
            raise IterationGraphNotFoundError(msg) from exc
