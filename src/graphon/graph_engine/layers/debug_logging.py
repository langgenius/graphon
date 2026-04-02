"""Debug logging layer for GraphEngine.

This module provides a layer that logs all events and state changes during
graph execution for debugging purposes.
"""

import logging
from collections.abc import Mapping
from typing import Any, final, override

from graphon.graph_events.base import GraphEngineEvent
from graphon.graph_events.graph import (
    GraphRunAbortedEvent,
    GraphRunFailedEvent,
    GraphRunPartialSucceededEvent,
    GraphRunStartedEvent,
    GraphRunSucceededEvent,
)
from graphon.graph_events.iteration import (
    NodeRunIterationFailedEvent,
    NodeRunIterationNextEvent,
    NodeRunIterationStartedEvent,
    NodeRunIterationSucceededEvent,
)
from graphon.graph_events.loop import (
    NodeRunLoopFailedEvent,
    NodeRunLoopNextEvent,
    NodeRunLoopStartedEvent,
    NodeRunLoopSucceededEvent,
)
from graphon.graph_events.node import (
    NodeRunExceptionEvent,
    NodeRunFailedEvent,
    NodeRunRetryEvent,
    NodeRunStartedEvent,
    NodeRunStreamChunkEvent,
    NodeRunSucceededEvent,
)

from .base import GraphEngineLayer


@final
class DebugLoggingLayer(GraphEngineLayer):
    """A layer that provides comprehensive logging of GraphEngine execution.

    This layer logs all events with configurable detail levels, helping developers
    debug workflow execution and understand the flow of events.
    """

    def __init__(
        self,
        level: str = "INFO",
        include_inputs: bool = False,
        include_outputs: bool = True,
        include_process_data: bool = False,
        logger_name: str = "GraphEngine.Debug",
        max_value_length: int = 500,
    ) -> None:
        """Initialize the debug logging layer.

        Args:
            level: Logging level (DEBUG, INFO, WARNING, ERROR)
            include_inputs: Whether to log node input values
            include_outputs: Whether to log node output values
            include_process_data: Whether to log node process data
            logger_name: Name of the logger to use
            max_value_length: Maximum length of logged values (truncated if longer)

        """
        super().__init__()
        self.level = level
        self.include_inputs = include_inputs
        self.include_outputs = include_outputs
        self.include_process_data = include_process_data
        self.max_value_length = max_value_length

        # Set up logger
        self.logger = logging.getLogger(logger_name)
        log_level = getattr(logging, level.upper(), logging.INFO)
        self.logger.setLevel(log_level)

        # Track execution stats
        self.node_count = 0
        self.success_count = 0
        self.failure_count = 0
        self.retry_count = 0

    def _truncate_value(self, value: Any) -> str:
        """Truncate long values for logging."""
        str_value = str(value)
        if len(str_value) > self.max_value_length:
            return str_value[: self.max_value_length] + "... (truncated)"
        return str_value

    def _format_dict(self, data: dict[str, Any] | Mapping[str, Any]) -> str:
        """Format a dictionary or mapping for logging with truncation."""
        if not data:
            return "{}"

        formatted_items: list[str] = []
        for key, value in data.items():
            formatted_value = self._truncate_value(value)
            formatted_items.append(f"  {key}: {formatted_value}")

        return f"""{{
{",\n".join(formatted_items)}
}}"""

    @override
    def on_graph_start(self) -> None:
        """Log graph execution start."""
        self.logger.info("=" * 80)
        self.logger.info("🚀 GRAPH EXECUTION STARTED")
        self.logger.info("=" * 80)
        # Log initial state
        self.logger.info("Initial State:")

    @override
    def on_event(self, event: GraphEngineEvent) -> None:
        """Log individual events based on their type."""
        event_class = event.__class__.__name__
        match event:
            case GraphRunStartedEvent():
                self._log_graph_run_started_event(event)
            case GraphRunSucceededEvent():
                self._log_graph_run_succeeded_event(event)
            case GraphRunPartialSucceededEvent():
                self._log_graph_run_partial_succeeded_event(event)
            case GraphRunFailedEvent():
                self._log_graph_run_failed_event(event)
            case GraphRunAbortedEvent():
                self._log_graph_run_aborted_event(event)
            case NodeRunRetryEvent():
                self._log_node_run_retry_event(event)
            case NodeRunStartedEvent():
                self._log_node_run_started_event(event)
            case NodeRunSucceededEvent():
                self._log_node_run_succeeded_event(event)
            case NodeRunFailedEvent():
                self._log_node_run_failed_event(event)
            case NodeRunExceptionEvent():
                self._log_node_run_exception_event(event)
            case NodeRunStreamChunkEvent():
                self._log_node_run_stream_chunk_event(event)
            case NodeRunIterationStartedEvent():
                self._log_iteration_started_event(event)
            case NodeRunIterationNextEvent():
                self._log_iteration_next_event(event)
            case NodeRunIterationSucceededEvent():
                self._log_iteration_succeeded_event(event)
            case NodeRunIterationFailedEvent():
                self._log_iteration_failed_event(event)
            case NodeRunLoopStartedEvent():
                self._log_loop_started_event(event)
            case NodeRunLoopNextEvent():
                self._log_loop_next_event(event)
            case NodeRunLoopSucceededEvent():
                self._log_loop_succeeded_event(event)
            case NodeRunLoopFailedEvent():
                self._log_loop_failed_event(event)
            case _:
                self.logger.debug("Event: %s", event_class)

    def _log_graph_run_started_event(self, event: GraphRunStartedEvent) -> None:
        _ = event
        self.logger.debug("Graph run started event")

    def _log_graph_run_succeeded_event(self, event: GraphRunSucceededEvent) -> None:
        self.logger.info("✅ Graph run succeeded")
        if self.include_outputs and event.outputs:
            self.logger.info("  Final outputs: %s", self._format_dict(event.outputs))

    def _log_graph_run_partial_succeeded_event(
        self,
        event: GraphRunPartialSucceededEvent,
    ) -> None:
        self.logger.warning("⚠️ Graph run partially succeeded")
        if event.exceptions_count > 0:
            self.logger.warning("  Total exceptions: %s", event.exceptions_count)
        if self.include_outputs and event.outputs:
            self.logger.info("  Final outputs: %s", self._format_dict(event.outputs))

    def _log_graph_run_failed_event(self, event: GraphRunFailedEvent) -> None:
        self.logger.error("❌ Graph run failed: %s", event.error)
        if event.exceptions_count > 0:
            self.logger.error("  Total exceptions: %s", event.exceptions_count)

    def _log_graph_run_aborted_event(self, event: GraphRunAbortedEvent) -> None:
        self.logger.warning("⚠️ Graph run aborted: %s", event.reason)
        if event.outputs:
            self.logger.info("  Partial outputs: %s", self._format_dict(event.outputs))

    def _log_node_run_retry_event(self, event: NodeRunRetryEvent) -> None:
        self.retry_count += 1
        self.logger.warning(
            "🔄 Node retry: %s (attempt %s)",
            event.node_id,
            event.retry_index,
        )
        self.logger.warning("  Previous error: %s", event.error)

    def _log_node_run_started_event(self, event: NodeRunStartedEvent) -> None:
        self.node_count += 1
        self.logger.info(
            '▶️ Node started: %s - "%s" (type: %s)',
            event.node_id,
            event.node_title,
            event.node_type,
        )
        if self.include_inputs and event.node_run_result.inputs:
            self.logger.debug(
                "  Inputs: %s",
                self._format_dict(event.node_run_result.inputs),
            )

    def _log_node_run_succeeded_event(self, event: NodeRunSucceededEvent) -> None:
        self.success_count += 1
        self.logger.info("✅ Node succeeded: %s", event.node_id)
        if self.include_outputs and event.node_run_result.outputs:
            self.logger.debug(
                "  Outputs: %s",
                self._format_dict(event.node_run_result.outputs),
            )
        if self.include_process_data and event.node_run_result.process_data:
            self.logger.debug(
                "  Process data: %s",
                self._format_dict(event.node_run_result.process_data),
            )

    def _log_node_run_failed_event(self, event: NodeRunFailedEvent) -> None:
        self.failure_count += 1
        self.logger.error("❌ Node failed: %s", event.node_id)
        self.logger.error("  Error: %s", event.error)
        if event.node_run_result.error:
            self.logger.error("  Details: %s", event.node_run_result.error)

    def _log_node_run_exception_event(self, event: NodeRunExceptionEvent) -> None:
        self.logger.warning("⚠️ Node exception handled: %s", event.node_id)
        self.logger.warning("  Error: %s", event.error)

    def _log_node_run_stream_chunk_event(self, event: NodeRunStreamChunkEvent) -> None:
        final_indicator = " (FINAL)" if event.is_final else ""
        self.logger.debug(
            "📝 Stream chunk from %s%s: %s",
            event.node_id,
            final_indicator,
            self._truncate_value(event.chunk),
        )

    def _log_iteration_started_event(self, event: NodeRunIterationStartedEvent) -> None:
        self.logger.info("🔁 Iteration started: %s", event.node_id)

    def _log_iteration_next_event(self, event: NodeRunIterationNextEvent) -> None:
        self.logger.debug(
            "  Iteration next: %s (index: %s)",
            event.node_id,
            event.index,
        )

    def _log_iteration_succeeded_event(
        self,
        event: NodeRunIterationSucceededEvent,
    ) -> None:
        self.logger.info("✅ Iteration succeeded: %s", event.node_id)
        if self.include_outputs and event.outputs:
            self.logger.debug("  Outputs: %s", self._format_dict(event.outputs))

    def _log_iteration_failed_event(self, event: NodeRunIterationFailedEvent) -> None:
        self.logger.error("❌ Iteration failed: %s", event.node_id)
        self.logger.error("  Error: %s", event.error)

    def _log_loop_started_event(self, event: NodeRunLoopStartedEvent) -> None:
        self.logger.info("🔄 Loop started: %s", event.node_id)

    def _log_loop_next_event(self, event: NodeRunLoopNextEvent) -> None:
        self.logger.debug(
            "  Loop iteration: %s (index: %s)",
            event.node_id,
            event.index,
        )

    def _log_loop_succeeded_event(self, event: NodeRunLoopSucceededEvent) -> None:
        self.logger.info("✅ Loop succeeded: %s", event.node_id)
        if self.include_outputs and event.outputs:
            self.logger.debug("  Outputs: %s", self._format_dict(event.outputs))

    def _log_loop_failed_event(self, event: NodeRunLoopFailedEvent) -> None:
        self.logger.error("❌ Loop failed: %s", event.node_id)
        self.logger.error("  Error: %s", event.error)

    @override
    def on_graph_end(self, error: Exception | None) -> None:
        """Log graph execution end with summary statistics."""
        self.logger.info("=" * 80)

        if error:
            self.logger.error("🔴 GRAPH EXECUTION FAILED")
            self.logger.error("  Error: %s", error)
        else:
            self.logger.info("🎉 GRAPH EXECUTION COMPLETED SUCCESSFULLY")

        # Log execution statistics
        self.logger.info("Execution Statistics:")
        self.logger.info("  Total nodes executed: %s", self.node_count)
        self.logger.info("  Successful nodes: %s", self.success_count)
        self.logger.info("  Failed nodes: %s", self.failure_count)
        self.logger.info("  Node retries: %s", self.retry_count)

        # Log final state if available
        if self.include_outputs and self.graph_runtime_state.outputs:
            self.logger.info(
                "Final outputs: %s",
                self._format_dict(self.graph_runtime_state.outputs),
            )

        self.logger.info("=" * 80)
