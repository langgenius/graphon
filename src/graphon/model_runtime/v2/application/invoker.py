from typing import Protocol

from graphon.model_runtime.v2.application.context import CallContext
from graphon.model_runtime.v2.domain.exchange import ModelRequest, ModelResult


class ModelInvoker(Protocol):
    def invoke(self, request: ModelRequest, *, context: CallContext) -> ModelResult: ...
