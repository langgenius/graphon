from copy import deepcopy
from dataclasses import dataclass, field

from graphon.model_runtime.v2.domain.json_values import JsonObject, copy_json_object


@dataclass(frozen=True, init=False)
class DataFormat:
    _schema: JsonObject = field(repr=False)
    kinds: tuple[str, ...]
    profile: str | None

    def __init__(
        self,
        *,
        schema: JsonObject,
        kinds: tuple[str, ...],
        profile: str | None = None,
    ) -> None:
        if (
            not isinstance(kinds, tuple)
            or not all(isinstance(kind, str) for kind in kinds)
            or (profile is not None and not isinstance(profile, str))
        ):
            message = (
                "Kinds must be a tuple of strings; profile must be a string or None"
            )
            raise ValueError(message)
        object.__setattr__(self, "_schema", copy_json_object(schema))
        object.__setattr__(self, "kinds", kinds)
        object.__setattr__(self, "profile", profile)

    @property
    def schema(self) -> JsonObject:
        return deepcopy(self._schema)
