from dataclasses import dataclass
from math import isfinite

from graphon.model_runtime.v2.domain.identity import require_identifier


@dataclass(frozen=True, slots=True, kw_only=True)
class CallContext:
    connection_id: str
    request_id: str
    timeout_seconds: float | None = None

    def __post_init__(self) -> None:
        require_identifier(self.connection_id)
        require_identifier(self.request_id)
        if self.timeout_seconds is not None and (
            isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, (int, float))
            or not isfinite(self.timeout_seconds)
            or self.timeout_seconds <= 0
        ):
            message = "Timeout must be a positive finite number of seconds"
            raise ValueError(message)
