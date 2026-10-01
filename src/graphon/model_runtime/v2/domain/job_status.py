from dataclasses import dataclass, field
from datetime import datetime, timedelta
from math import isfinite
from typing import Literal

from graphon.model_runtime.v2.domain.errors import ModelError
from graphon.model_runtime.v2.domain.exchange import ModelResult
from graphon.model_runtime.v2.domain.identity import (
    ContractRef,
    ModelRef,
    require_identifier,
)
from graphon.model_runtime.v2.domain.json_values import JsonValue, copy_json, copy_usage


@dataclass(frozen=True, slots=True, kw_only=True)
class JobRef:
    model: ModelRef
    contract: ContractRef
    request_id: str
    id: str
    scope: str
    token: JsonValue = field(repr=False)

    def __post_init__(self) -> None:
        for value in (self.request_id, self.id, self.scope):
            require_identifier(value)
        object.__setattr__(self, "token", copy_json(self.token))


@dataclass(frozen=True, slots=True, kw_only=True)
class JobStatus:
    job: JobRef
    status: Literal["queued", "running", "succeeded", "failed", "cancelled"]
    cancellable: bool
    result: ModelResult | None = None
    error: ModelError | None = None
    next_check_after_seconds: float | None = None
    expires_at: datetime | None = None
    usage: JsonValue = 0

    def __post_init__(self) -> None:
        if self.status not in {"queued", "running", "succeeded", "failed", "cancelled"}:
            message = "Unknown job status"
            raise ValueError(message)
        if (self.result is not None) != (self.status == "succeeded") or (
            self.error is not None
        ) != (self.status == "failed"):
            message = "Job result and error must match its status"
            raise ValueError(message)
        if self.status in {"succeeded", "failed", "cancelled"} and self.cancellable:
            message = "Terminal jobs cannot be cancellable"
            raise ValueError(message)
        if self.result is not None and (
            self.result.model,
            self.result.contract,
            self.result.request_id,
        ) != (self.job.model, self.job.contract, self.job.request_id):
            message = "Job result must match the submission identity"
            raise ValueError(message)
        if self.next_check_after_seconds is not None and (
            isinstance(self.next_check_after_seconds, bool)
            or not isinstance(self.next_check_after_seconds, (int, float))
            or not isfinite(self.next_check_after_seconds)
            or self.next_check_after_seconds <= 0
        ):
            message = "Next check delay must be a positive finite number of seconds"
            raise ValueError(message)
        if self.expires_at is not None and (
            not isinstance(self.expires_at, datetime)
            or self.expires_at.utcoffset() != timedelta(0)
        ):
            message = "Job expiry must be an aware UTC timestamp"
            raise ValueError(message)
        object.__setattr__(self, "usage", copy_usage(self.usage))
