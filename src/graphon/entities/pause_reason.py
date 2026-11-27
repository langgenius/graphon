from enum import StrEnum, auto
from typing import Annotated, Literal, cast

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field
from typing_extensions import deprecated


class PauseReasonType(StrEnum):
    # Legacy value for compatibility purpose, DO NOT USE.

    # LEGACY `human_input_required` pause reasons are normalized
    # into `HitlRequired`, and `form_id` are mapped to `session_id`
    # when deserializing.
    LEGACY_HUMAN_INPUT_REQUIRED = "human_input_required"

    HITL_REQUIRED = auto()
    SCHEDULED_PAUSE = auto()
    EXTERNAL_WAIT = auto()


@deprecated("Use ExternalWait for new pause reasons.", category=None)
class HitlRequired(BaseModel):
    """Deprecated human-input pause reason emitted by the built-in Human Input node."""

    model_config = ConfigDict(extra="forbid")

    TYPE: Literal[PauseReasonType.HITL_REQUIRED] = PauseReasonType.HITL_REQUIRED
    session_id: str
    node_id: str
    node_title: str


class SchedulingPause(BaseModel):
    TYPE: Literal[PauseReasonType.SCHEDULED_PAUSE] = PauseReasonType.SCHEDULED_PAUSE

    message: str


class ExternalWait(BaseModel):
    """General-purpose pause reason for a node awaiting a host-managed result.

    Nodes can use this reason to wait for human input or an external service.
    The host defines the meaning of ``wait_id`` for each ``wait_type`` and
    resumes the waiting execution when the result is ready.
    """

    model_config = ConfigDict(frozen=True)

    TYPE: Literal[PauseReasonType.EXTERNAL_WAIT] = PauseReasonType.EXTERNAL_WAIT
    # Hosts use this type to interpret wait_id. The dify.* namespace is
    # reserved for wait types defined by upstream Dify.
    # PEG grammar for wait_type:
    #   WAIT_TYPE  <- IDENTIFIER ('.' IDENTIFIER)* !.
    #   IDENTIFIER <- [A-Za-z_] [A-Za-z0-9_]*
    wait_type: str = Field(
        pattern=r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$"
    )
    # Opaque identifier of the work the host is waiting for.
    wait_id: str
    # ID of the node in the graph definition, shared across its executions.
    node_id: str = Field(min_length=1)
    # Display title of the node at the time it requested the pause.
    node_title: str
    # ID of the specific node execution that requested the pause.
    node_execution_id: str = Field(min_length=1)
    # ID of the execution frame containing that node execution.
    frame_id: str = Field(min_length=1)


class _LegacyHumanInputRequired(BaseModel):
    """
    This model serves as documentation purpose. DO NOT USE.
    """

    TYPE: Literal[PauseReasonType.LEGACY_HUMAN_INPUT_REQUIRED] = (
        PauseReasonType.LEGACY_HUMAN_INPUT_REQUIRED
    )
    form_id: str
    # the following fields are intentionally kept in text form
    # for documentation purpose only.
    #
    # form_content: str
    # inputs: list[FormInputConfig] = Field(default_factory=list[FormInputConfig])
    # actions: list[UserActionConfig] = Field(default_factory=list[UserActionConfig])
    # resolved_default_values: Mapping[str, Any] = Field(default_factory=dict)


def _decode_pause_reason(value: object) -> object:
    if not isinstance(value, dict):
        return value

    payload = cast(dict[str, object], value)

    if payload.get("TYPE") != PauseReasonType.LEGACY_HUMAN_INPUT_REQUIRED:
        return value

    canonical_reason: dict[str, object] = {
        "TYPE": PauseReasonType.HITL_REQUIRED,
    }
    if "form_id" in payload:
        canonical_reason["session_id"] = payload["form_id"]
    if "node_id" in payload:
        canonical_reason["node_id"] = payload["node_id"]
    if "node_title" in payload:
        canonical_reason["node_title"] = payload["node_title"]
    return canonical_reason


type PauseReason = Annotated[
    HitlRequired | SchedulingPause | ExternalWait,  # ty: ignore[deprecated]
    Field(discriminator="TYPE"),
    BeforeValidator(_decode_pause_reason),
]
