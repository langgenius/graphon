"""QueueBasedGraphEngine - Main orchestrator for queue-based workflow execution.

This engine uses a modular architecture with separated packages following
Domain-Driven Design principles for improved maintainability and testability.
"""

from __future__ import annotations

import logging
import queue
from collections.abc import Callable, Generator
from dataclasses import dataclass
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
from graphon.runtime.graph_runtime_state import (
    GraphExecutionProtocol,
    GraphRuntimeState,
)
from graphon.runtime.read_only_wrappers import ReadOnlyGraphRuntimeStateWrapper

from .command_channels import CommandChannel
from .command_processing import (
    AbortCommandHandler,
    CommandProcessor,
    PauseCommandHandler,
    UpdateVariablesCommandHandler,
)
from .config import GraphEngineConfig
from .container_handlers import ContainerHandler
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
from .orchestration import Dispatcher, ExecutionCoordinator
from .ready_queue import ROOT_FRAME_ID
from .worker_management import WorkerPool

logger = logging.getLogger(__name__)


_DEFAULT_CONFIG = GraphEngineConfig()


@dataclass(slots=True)
class _GraphRunLifecycle:
    graph_execution: GraphExecutionProtocol
    event_manager: EventManager
    initialize_layers: Callable[[], None]
    start_execution: Callable[..., None]
    stop_execution: Callable[[], None]
    emit_terminal_events: Callable[[], Generator[GraphEngineEvent, None, None]]
    is_resume: bool = False

    def run(self) -> Generator[GraphEngineEvent, None, None]:
        try:
            self._prepare()
            yield self._started_event()
            self.start_execution(resume=self.is_resume)
            yield from self.event_manager.emit_events()
            yield from self.emit_terminal_events()
        except Exception as error:
            yield self._failed_event(error)
            raise
        finally:
            self.stop_execution()

    def _prepare(self) -> None:
        self.initialize_layers()
        self.is_resume = self.graph_execution.started
        if self.is_resume:
            self._resume_graph_execution()
            return
        self.graph_execution.start()

    def _resume_graph_execution(self) -> None:
        self.graph_execution.paused = False
        self.graph_execution.pause_reasons = []

    def _started_event(self) -> GraphRunStartedEvent:
        event = GraphRunStartedEvent(reason=self._start_reason())
        self.event_manager.notify_layers(event)
        return event

    def _start_reason(self) -> WorkflowStartReason:
        if self.is_resume:
            return WorkflowStartReason.RESUMPTION
        return WorkflowStartReason.INITIAL

    def _failed_event(self, error: Exception) -> GraphRunFailedEvent:
        event = GraphRunFailedEvent(
            error=str(error),
            exceptions_count=self.graph_execution.exceptions_count,
        )
        self.event_manager.notify_layers(event)
        return event


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
        self._graph_runtime_state.configure(graph=graph)
        self._command_channel = command_channel
        self._config = config
        self._layers: list[GraphEngineLayer] = []

        # Graph execution tracks the overall execution state
        self._graph_execution = self._graph_runtime_state.graph_execution
        self._graph_execution.workflow_id = workflow_id

        # === Execution Queues ===
        self._ready_queue = self._graph_runtime_state.ready_queue

        # Queue for events generated during execution
        self._event_queue: queue.Queue[TaskEvent] = queue.Queue()

        # === State Management ===
        # Unified state manager handles all node state transitions and queue operations
        self._state_manager = GraphStateManager(self._graph, self._ready_queue)
        self._frame_registry = FrameRegistry()

        # === Event Management ===
        # Event manager handles both collection and emission of events
        self._event_manager = EventManager()

        # === Error Handling ===
        # Centralized error handler for graph execution errors
        self._error_handler = ErrorHandler(self._graph, self._graph_execution)

        # === Graph Traversal Components ===
        # Propagates skip status through the graph when conditions aren't met
        self._skip_propagator = SkipPropagator(
            graph=self._graph,
            state_manager=self._state_manager,
        )

        # Processes edges to determine next nodes after execution
        # Also handles conditional branching and route selection
        self._edge_processor = EdgeProcessor(
            graph=self._graph,
            state_manager=self._state_manager,
            skip_propagator=self._skip_propagator,
        )
        self._frame_registry.register(
            ExecutionFrame(
                frame_id=ROOT_FRAME_ID,
                graph=self._graph,
                graph_runtime_state=self._graph_runtime_state,
                state_manager=self._state_manager,
                edge_processor=self._edge_processor,
                error_handler=self._error_handler,
            ),
        )

        # === Command Processing ===
        # Processes external commands (e.g., abort requests)
        self._command_processor = CommandProcessor(
            command_channel=self._command_channel,
            graph_execution=self._graph_execution,
        )

        # Register command handlers
        abort_handler = AbortCommandHandler()
        self._command_processor.register_handler(AbortCommand, abort_handler)

        pause_handler = PauseCommandHandler()
        self._command_processor.register_handler(PauseCommand, pause_handler)

        update_variables_handler = UpdateVariablesCommandHandler(
            self._graph_runtime_state.variable_pool,
        )
        self._command_processor.register_handler(
            UpdateVariablesCommand,
            update_variables_handler,
        )

        # === Worker Pool Setup ===
        self._loop_container_handler = LoopContainerHandler(
            frame_registry=self._frame_registry,
            graph_execution=self._graph_execution,
        )
        self._iteration_container_handler = IterationContainerHandler(
            frame_registry=self._frame_registry,
            graph_execution=self._graph_execution,
        )
        self._container_handlers: dict[str, ContainerHandler] = {
            "loop": self._loop_container_handler,
            "iteration": self._iteration_container_handler,
        }

        # Create worker pool for parallel node execution
        self._worker_pool = WorkerPool(
            ready_queue=self._ready_queue,
            event_queue=self._event_queue,
            graph=self._graph,
            frame_registry=self._frame_registry,
            layers=self._layers,
            execution_context=self._graph_runtime_state.execution_context,
            config=self._config,
            container_handlers=self._container_handlers,
        )

        # === Orchestration ===
        # Coordinates the overall execution lifecycle
        self._execution_coordinator = ExecutionCoordinator(
            graph_execution=self._graph_execution,
            state_manager=self._state_manager,
            command_processor=self._command_processor,
            worker_pool=self._worker_pool,
        )

        # === Event Handler Registry ===
        # Central registry for handling all node execution events
        self._event_handler_registry = EventHandler(
            graph_execution=self._graph_execution,
            event_collector=self._event_manager,
            frame_registry=self._frame_registry,
            container_handlers=self._container_handlers,
        )

        # Dispatches events and manages execution flow
        self._dispatcher = Dispatcher(
            event_queue=self._event_queue,
            event_handler=self._event_handler_registry,
            execution_coordinator=self._execution_coordinator,
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

    def _bind_layer_context(
        self,
        layer: GraphEngineLayer,
    ) -> None:
        layer.initialize(
            ReadOnlyGraphRuntimeStateWrapper(self._graph_runtime_state),
            self._command_channel,
        )

    def layer(self, layer: GraphEngineLayer) -> GraphEngine:
        """Add a layer for extending functionality."""
        self._layers.append(layer)
        self._bind_layer_context(layer)
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
        lifecycle = _GraphRunLifecycle(
            graph_execution=self._graph_execution,
            event_manager=self._event_manager,
            initialize_layers=self._initialize_layers,
            start_execution=self._start_execution,
            stop_execution=self._stop_execution,
            emit_terminal_events=self._emit_terminal_events,
        )
        yield from lifecycle.run()

    def _emit_terminal_events(self) -> Generator[GraphEngineEvent, None, None]:
        if self._graph_execution.is_paused:
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

        if self._graph_execution.has_error:
            error = self._graph_execution.error
            if error is not None:
                raise error
            return

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

    def _start_execution(self, *, resume: bool = False) -> None:
        """Start execution subsystems."""
        paused_nodes: list[str] = []
        deferred_nodes: list[str] = []
        if resume:
            paused_nodes = self._graph_runtime_state.consume_paused_nodes()
            deferred_nodes = self._graph_runtime_state.consume_deferred_nodes()

        # Start worker pool (it calculates initial workers internally)
        self._worker_pool.start()

        if not resume:
            # Enqueue root node
            root_node = self._graph.root_node
            self._state_manager.enqueue_node(
                frame_id=ROOT_FRAME_ID,
                node_id=root_node.id,
            )
            self._state_manager.start_execution(
                frame_id=ROOT_FRAME_ID,
                node_id=root_node.id,
            )
        else:
            seen_nodes: set[str] = set()
            for node_id in paused_nodes + deferred_nodes:
                if node_id in seen_nodes:
                    continue
                seen_nodes.add(node_id)
                self._state_manager.enqueue_node(
                    frame_id=ROOT_FRAME_ID,
                    node_id=node_id,
                )
                self._state_manager.start_execution(
                    frame_id=ROOT_FRAME_ID,
                    node_id=node_id,
                )

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
