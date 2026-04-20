import pytest

from graphon.nodes.human_input.enums import FormInputType
from graphon.variables.input_entities import VariableEntityType


@pytest.mark.parametrize("form_input_type", FormInputType, ids=lambda item: item.name)
def test_form_input_type_members_exist_in_variable_entity_type(
    form_input_type: type[FormInputType],
) -> None:
    assert form_input_type.name in VariableEntityType.__members__
