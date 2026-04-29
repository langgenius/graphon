from graphon.graph_engine.command_processing.command_handlers import (
    UpdateVariablesCommandHandler,
)
from graphon.graph_engine.domain.graph_execution import GraphExecution
from graphon.graph_engine.entities.commands import (
    UpdateVariablesCommand,
    VariableUpdate,
)
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.variables import StringVariable


def test_update_variables_command_preserves_existing_writable_flag() -> None:
    variable_pool = VariablePool.empty()
    variable_pool.add(("conversation", "session_name"), "before", writable=True)

    handler = UpdateVariablesCommandHandler(variable_pool)
    command = UpdateVariablesCommand(
        updates=[
            VariableUpdate(
                value=StringVariable(
                    name="session_name",
                    selector=["conversation", "session_name"],
                    value="after",
                ),
            ),
        ],
    )

    handler.handle(command, GraphExecution(workflow_id="wf-123"))

    updated_variable = variable_pool.get_variable(("conversation", "session_name"))
    assert updated_variable is not None
    assert updated_variable.value == "after"
    assert updated_variable.writable is True
