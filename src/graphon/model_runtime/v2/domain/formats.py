from copy import deepcopy
from dataclasses import dataclass, field

from graphon.model_runtime.v2.domain.json_values import JsonObject, copy_json_object


@dataclass(frozen=True, init=False)
class DataFormat:
    _schema: JsonObject = field(repr=False)
    mime_types: tuple[str, ...]
    profile: str | None

    def __init__(
        self,
        *,
        schema: JsonObject,
        mime_types: tuple[str, ...],
        profile: str | None = None,
    ) -> None:
        if (
            not isinstance(mime_types, tuple)
            or not all(isinstance(mime_type, str) for mime_type in mime_types)
            or (profile is not None and not isinstance(profile, str))
        ):
            message = (
                "MIME types must be a tuple of strings; "
                "profile must be a string or None"
            )
            raise ValueError(message)
        object.__setattr__(self, "_schema", copy_json_object(schema))
        object.__setattr__(self, "mime_types", mime_types)
        object.__setattr__(self, "profile", profile)

    @property
    def schema(self) -> JsonObject:
        return deepcopy(self._schema)
