from graphon.model_runtime.v2.domain.errors import ModelError
from graphon.model_runtime.v2.domain.json_values import JsonValue, copy_usage


class ModelCallError(Exception):
    def __init__(self, *, error: ModelError, usage: JsonValue = 0) -> None:
        self.error = error
        self.usage = copy_usage(usage)
        super().__init__(error.message)
