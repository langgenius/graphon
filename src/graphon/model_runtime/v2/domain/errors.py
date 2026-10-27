from dataclasses import dataclass
from math import isfinite

from graphon.model_runtime.v2.domain.identity import require_identifier


@dataclass(frozen=True, slots=True, kw_only=True)
class ModelError:
    code: str
    message: str
    provider_code: str | None = None
    retry_after_seconds: float | None = None

    def __post_init__(self) -> None:
        require_identifier(self.code)
        if not isinstance(self.message, str) or (
            self.provider_code is not None and not isinstance(self.provider_code, str)
        ):
            message = "Error message and provider code must be strings"
            raise TypeError(message)
        if self.retry_after_seconds is not None and (
            isinstance(self.retry_after_seconds, bool)
            or not isinstance(self.retry_after_seconds, (int, float))
            or not isfinite(self.retry_after_seconds)
            or self.retry_after_seconds < 0
        ):
            message = "Retry delay must be a nonnegative finite number of seconds"
            raise ValueError(message)
