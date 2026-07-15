"""QueueBasedGraphEngine - Main orchestrator for queue-based workflow execution.

This engine uses a modular architecture with separated packages following
Domain-Driven Design principles for improved maintainability and testability.
"""

from __future__ import annotations

import logging
import queue
from collections.abc import Generator
from typing import final

from graphon.entities.workflow_start_reason import WorkflowStartReason
from graphon.graph.graph import Graph
from graphon.graph_events.base import (
    GraphEngineEvent,
)
from graphon.graph_events.graph import (
    GraphRunAbortedEvent,
    GraphRunFailedEvent,
    GraphRunPartialSucceededEvent,
    GraphRunPausedEvent,
    GraphRunStartedEvent,
    GraphRunSucceededEvent,
)
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.runtime.read_only_wrappers import ReadOnlyGraphRuntimeStateWrapper

from .command_channels import CommandChannel
from .command_processing import (
    AbortCommandHandler,
    CommandProcessor,
    PauseCommandHandler,
    UpdateVariablesCommandHandler,
)
from .config import GraphEngineConfig
from .entities.commands import AbortCommand, PauseCommand, UpdateVariablesCommand
from .entities.tasks import TaskEvent
from .error_handler import ErrorHandler
from .event_management import EventHandler, EventManager
from .frames import ExecutionFrame, FrameRegistry
from .graph_state_manager import GraphStateManager
from .graph_traversal import EdgeProcessor, SkipPropagator
from .iteration_container_handler import IterationContainerHandler
from .layers.base import GraphEngineLayer
from .loop_container_handler import LoopContainerHandler
from .orchestration import Dispatcher
from .ready_queue import ROOT_FRAME_ID, StartTask
from .worker_management import WorkerPool

logger = logging.getLogger(__name__)


_DEFAULT_CONFIG = GraphEngineConfig()


@final
class GraphEngine:
    """Queue-based graph execution engine.

    Uses a modular architecture that delegates responsibilities to specialized
    subsystems, following Domain-Driven Design and SOLID principles.
    """

    def __init__(
        self,
        workflow_id: str,
        graph: Graph,
        graph_runtime_state: GraphRuntimeState,
        command_channel: CommandChannel,
        config: GraphEngineConfig = _DEFAULT_CONFIG,
    ) -> None:
        """Initialize the graph engine with all subsystems and dependencies."""
        # Bind runtime state to current workflow context
        self._graph = graph
        self._graph_runtime_state = graph_runtime_state
        self._graph_runtime_state.attach_graph(graph)
        self._command_channel = command_channel
        self._layers: list[GraphEngineLayer] = []

        # Graph execution tracks the overall execution state
        self._graph_execution = self._graph_runtime_state.graph_execution
        self._graph_execution.workflow_id = workflow_id

        # Queue for events generated during execution
        event_queue: queue.Queue[TaskEvent] = queue.Queue()

        # === State Management ===
        # Unified state manager handles all node state transitions and queue operations
        self._state_manager = GraphStateManager(
            self._graph,
            self._graph_runtime_state,
            ROOT_FRAME_ID,
        )
        self._frame_registry = FrameRegistry()

        # === Event Management ===
        # Event manager handles both collection and emission of events
        self._event_manager = EventManager()

        # === Error Handling ===
        # Centralized error handler for graph execution errors
        error_handler = ErrorHandler(self._graph, self._graph_execution)

        # === Graph Traversal Components ===
        # Propagates skip status through the graph when conditions aren't met
        skip_propagator = SkipPropagator(
            graph=self._graph,
            state_manager=self._state_manager,
        )

        # Processes edges to determine next nodes after execution
        # Also handles conditional branching and route selection
        edge_processor = EdgeProcessor(
            graph=self._graph,
            state_manager=self._state_manager,
            skip_propagator=skip_propagator,
        )
        self._frame_registry.register(
            ExecutionFrame(
                frame_id=ROOT_FRAME_ID,
                graph=self._graph,
                graph_runtime_state=self._graph_runtime_state,
                state_manager=self._state_manager,
                edge_processor=edge_processor,
                error_handler=error_handler,
            ),
        )

        # === Command Processing ===
        # Processes external commands (e.g., abort requests)
        command_processor = CommandProcessor(
            command_channel=self._command_channel,
            graph_execution=self._graph_execution,
        )

        # Register command handlers
        command_processor.register_handler(AbortCommand, AbortCommandHandler())
        command_processor.register_handler(PauseCommand, PauseCommandHandler())
        command_processor.register_handler(
            UpdateVariablesCommand,
            UpdateVariablesCommandHandler(self._graph_runtime_state.variable_pool),
        )

        # === Worker Pool Setup ===
        self._container_handlers = {
            "loop": LoopContainerHandler(
                frame_registry=self._frame_registry,
            ),
            "iteration": IterationContainerHandler(
                frame_registry=self._frame_registry,
            ),
        }

        # Create worker pool for parallel node execution
        self._worker_pool = WorkerPool(
            ready_queue=self._graph_runtime_state.ready_queue,
            event_queue=event_queue,
            frame_registry=self._frame_registry,
            layers=self._layers,
            execution_context=self._graph_runtime_state.execution_context,
            config=config,
            container_handlers=self._container_handlers,
        )

        # === Event Handler Registry ===
        # Central registry for handling all node execution events
        event_handler = EventHandler(
            graph_execution=self._graph_execution,
            event_collector=self._event_manager,
            frame_registry=self._frame_registry,
            container_handlers=self._container_handlers,
        )

        # Dispatches events and manages execution flow
        self._dispatcher = Dispatcher(
            event_queue=event_queue,
            event_handler=event_handler,
            graph_execution=self._graph_execution,
            state_manager=self._state_manager,
            command_processor=command_processor,
            worker_pool=self._worker_pool,
            event_emitter=self._event_manager,
        )

        # === Validation ===
        # Ensure all nodes share the same GraphRuntimeState instance
        self._validate_graph_state_consistency()

    def _validate_graph_state_consistency(self) -> None:
        """Validate that all nodes share the same GraphRuntimeState."""
        expected_state_id = id(self._graph_runtime_state)
        for node in self._graph.nodes.values():
            if id(node.graph_runtime_state) != expected_state_id:
                msg = (
                    "GraphRuntimeState consistency violation: Node "
                    f"'{node.id}' has a different instance"
                )
                raise ValueError(msg)

    def layer(self, layer: GraphEngineLayer) -> GraphEngine:
        """Add a layer for extending functionality."""
        self._layers.append(layer)
        layer.initialize(
            ReadOnlyGraphRuntimeStateWrapper(self._graph_runtime_state),
            self._command_channel,
        )
        return self

    def request_abort(self, reason: str | None = None) -> None:
        """Queue an abort command for this engine."""
        self._command_channel.send_command(
            AbortCommand(reason=reason or "User requested abort"),
        )

    def run(self) -> Generator[GraphEngineEvent, None, None]:
        """Execute the graph using the modular architecture.

        Yields:
            `GraphEngineEvent` instances emitted during workflow execution.

        """
        try:
            yield from self._run_graph()
        except Exception as error:
            failed_event = GraphRunFailedEvent(
                error=str(error),
                exceptions_count=self._graph_execution.exceptions_count,
            )
            self._event_manager.notify_layers(failed_event)
            yield failed_event
            raise
        finally:
            self._stop_execution()

    def _run_graph(self) -> Generator[GraphEngineEvent, None, None]:
        self._event_manager.reset()
        self._initialize_layers()
        resume = self._graph_execution.started
        if resume:
            self._graph_execution.paused = False
            self._graph_execution.pause_reasons = []
        else:
            self._graph_execution.start()

        started_event = GraphRunStartedEvent(
            reason=(
                WorkflowStartReason.RESUMPTION
                if resume
                else WorkflowStartReason.INITIAL
            ),
        )
        self._event_manager.notify_layers(started_event)
        yield started_event
        self._start_execution(resume=resume)
        yield from self._event_manager.emit_events()
        yield from self._emit_terminal_events()

    def _emit_terminal_events(self) -> Generator[GraphEngineEvent, None, None]:
        if self._graph_execution.paused:
            pause_reasons = self._graph_execution.pause_reasons
            if not pause_reasons:
                msg = "pause_reasons should not be empty when execution is paused."
                raise RuntimeError(msg)
            # Ensure we have a valid PauseReason for the event
            paused_event = GraphRunPausedEvent(
                reasons=pause_reasons,
                outputs=self._graph_runtime_state.outputs,
            )
            self._event_manager.notify_layers(paused_event)
            yield paused_event
            return

        if self._graph_execution.aborted:
            abort_reason = "Workflow execution aborted by user command"
            if self._graph_execution.error:
                abort_reason = str(self._graph_execution.error)
            aborted_event = GraphRunAbortedEvent(
                reason=abort_reason,
                outputs=self._graph_runtime_state.outputs,
            )
            self._event_manager.notify_layers(aborted_event)
            yield aborted_event
            return

        if self._graph_execution.error is not None:
            raise self._graph_execution.error

        outputs = self._graph_runtime_state.outputs
        exceptions_count = self._graph_execution.exceptions_count
        if exceptions_count > 0:
            partial_event = GraphRunPartialSucceededEvent(
                exceptions_count=exceptions_count,
                outputs=outputs,
            )
            self._event_manager.notify_layers(partial_event)
            yield partial_event
            return

        succeeded_event = GraphRunSucceededEvent(
            outputs=outputs,
        )
        self._event_manager.notify_layers(succeeded_event)
        yield succeeded_event

    def _initialize_layers(self) -> None:
        """Initialize layers with context."""
        self._event_manager.set_layers(self._layers)
        for layer in self._layers:
            try:
                layer.on_graph_start()
            except Exception:
                logger.exception(
                    "Layer %s failed on_graph_start",
                    layer.__class__.__name__,
                )

    def _start_execution(self, *, resume: bool) -> None:
        """Start execution subsystems."""
        if resume:
            for frame_state in self._graph_runtime_state.container_frames():
                self._container_handlers[frame_state.kind].restore_frame(frame_state)
            for run_state in self._graph_runtime_state.container_runs():
                self._frame_registry.get(
                    run_state.frame_id,
                ).state_manager.track_unfinished(run_state.node_id)

        if not resume:
            # Enqueue root node
            root_node = self._graph.root_node
            self._state_manager.enqueue_node(root_node.id)
        else:
            for task in self._graph_runtime_state.drain_deferred_ready_tasks():
                self._graph_runtime_state.enqueue_ready_task(task)
                if isinstance(task, StartTask):
                    self._frame_registry.get(
                        task.frame_id
                    ).state_manager.track_unfinished(
                        task.node_id,
                    )

        # Start worker pool after scheduling is stable.
        self._worker_pool.start()

        # Start dispatcher
        self._dispatcher.start()

    def _stop_execution(self) -> None:
        """Stop execution subsystems."""
        self._dispatcher.stop()
        self._worker_pool.stop()
        # Don't mark complete here as the dispatcher already does it

        # Notify layers
        for layer in self._layers:
            try:
                layer.on_graph_end(self._graph_execution.error)
            except Exception:
                logger.exception(
                    "Layer %s failed on_graph_end",
                    layer.__class__.__name__,
                )

    # Public property accessors for attributes that need external access
    @property
    def graph(self) -> Graph:
        """Get the graph bound to this engine."""
        return self._graph

    @property
    def graph_runtime_state(self) -> GraphRuntimeState:
        """Get the graph runtime state."""
        return self._graph_runtime_state
