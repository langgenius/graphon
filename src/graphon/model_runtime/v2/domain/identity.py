from dataclasses import dataclass


def require_identifier(value: str) -> None:
    if not isinstance(value, str) or not value:
        message = "Identifier must be a nonempty string"
        raise ValueError(message)


@dataclass(frozen=True, slots=True, kw_only=True)
class ModelRef:
    plugin_id: str
    provider: str
    model: str

    def __post_init__(self) -> None:
        for value in (self.plugin_id, self.provider, self.model):
            require_identifier(value)


@dataclass(frozen=True, slots=True, kw_only=True)
class ContractRef:
    id: str
    revision: str

    def __post_init__(self) -> None:
        for value in (self.id, self.revision):
            require_identifier(value)
