import logging
from typing import final, override

from graphon.entities.pause_reason import SchedulingPause
from graphon.runtime.graph_runtime_state import GraphExecutionProtocol
from graphon.runtime.variable_pool import VariablePool

from ..entities.commands import (
    AbortCommand,
    PauseCommand,
    UpdateVariablesCommand,
)
from .command_processor import CommandHandler

logger = logging.getLogger(__name__)


@final
class AbortCommandHandler(CommandHandler[AbortCommand]):
    @override
    def handle(
        self,
        command: AbortCommand,
        execution: GraphExecutionProtocol,
    ) -> None:
        logger.debug("Aborting workflow %s: %s", execution.workflow_id, command.reason)
        execution.abort(command.reason or "User requested abort")


@final
class PauseCommandHandler(CommandHandler[PauseCommand]):
    @override
    def handle(
        self,
        command: PauseCommand,
        execution: GraphExecutionProtocol,
    ) -> None:
        logger.debug("Pausing workflow %s: %s", execution.workflow_id, command.reason)
        # Convert string reason to PauseReason if needed
        reason = command.reason
        pause_reason = SchedulingPause(message=reason)
        execution.pause(pause_reason)


@final
class UpdateVariablesCommandHandler(CommandHandler[UpdateVariablesCommand]):
    def __init__(self, variable_pool: VariablePool) -> None:
        self._variable_pool = variable_pool

    @override
    def handle(
        self,
        command: UpdateVariablesCommand,
        execution: GraphExecutionProtocol,
    ) -> None:
        for update in command.updates:
            try:
                variable = update.value
                self._variable_pool.add(variable.selector, variable)
                logger.debug(
                    "Updated variable %s for workflow %s",
                    variable.selector,
                    execution.workflow_id,
                )
            except ValueError as exc:
                logger.warning(
                    "Skipping invalid variable selector %s for workflow %s: %s",
                    getattr(update.value, "selector", None),
                    execution.workflow_id,
                    exc,
                )
