from enum import StrEnum, auto
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field


class PauseReasonType(StrEnum):
    HITL_REQUIRED = auto()
    SCHEDULED_PAUSE = auto()


class HitlRequired(BaseModel):
    model_config = ConfigDict(extra="forbid")

    TYPE: Literal[PauseReasonType.HITL_REQUIRED] = PauseReasonType.HITL_REQUIRED
    session_id: str
    node_id: str
    node_title: str


class SchedulingPause(BaseModel):
    TYPE: Literal[PauseReasonType.SCHEDULED_PAUSE] = PauseReasonType.SCHEDULED_PAUSE

    message: str


type PauseReason = Annotated[
    HitlRequired | SchedulingPause,
    Field(discriminator="TYPE"),
]
