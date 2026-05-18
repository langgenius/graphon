"""Main command processor for handling external commands."""

import logging
from abc import abstractmethod
from collections.abc import Callable
from typing import Protocol, final

from graphon.runtime.graph_runtime_state import GraphExecutionProtocol

from ..command_channels import CommandChannel
from ..entities.commands import GraphEngineCommand

logger = logging.getLogger(__name__)


class CommandHandler[CommandT: GraphEngineCommand](Protocol):
    """Protocol for command handlers."""

    @abstractmethod
    def handle(
        self,
        command: CommandT,
        execution: GraphExecutionProtocol,
    ) -> None: ...


@final
class CommandProcessor:
    """Processes external commands sent to the engine.

    This polls the command channel and dispatches commands to
    appropriate handlers.
    """

    def __init__(
        self,
        command_channel: CommandChannel,
        graph_execution: GraphExecutionProtocol,
    ) -> None:
        """Initialize the command processor.

        Args:
            command_channel: Channel for receiving commands
            graph_execution: Graph execution aggregate

        """
        self._command_channel = command_channel
        self._graph_execution = graph_execution
        self._handlers: dict[
            type[GraphEngineCommand],
            Callable[[GraphEngineCommand, GraphExecutionProtocol], None],
        ] = {}

    def register_handler[CommandT: GraphEngineCommand](
        self,
        command_type: type[CommandT],
        handler: CommandHandler[CommandT],
    ) -> None:
        """Register a handler for a command type.

        Args:
            command_type: Type of command to handle
            handler: Handler for the command

        """

        def invoke(
            command: GraphEngineCommand,
            execution: GraphExecutionProtocol,
        ) -> None:
            if not isinstance(command, command_type):
                msg = (
                    f"Registered handler for {command_type.__name__} received "
                    f"{type(command).__name__}"
                )
                raise TypeError(msg)
            handler.handle(command, execution)

        self._handlers[command_type] = invoke

    def process_commands(self) -> None:
        """Check for and process any pending commands."""
        try:
            commands = self._command_channel.fetch_commands()
            for command in commands:
                self._handle_command(command)
        except Exception:
            logger.exception("Error processing commands")

    def _handle_command(self, command: GraphEngineCommand) -> None:
        """Handle a single command.

        Args:
            command: The command to handle

        """
        handler = self._handlers.get(type(command))
        if handler:
            try:
                handler(command, self._graph_execution)
            except Exception:
                logger.exception(
                    "Error handling command %s",
                    command.__class__.__name__,
                )
        else:
            logger.warning(
                "No handler registered for command: %s",
                command.__class__.__name__,
            )
