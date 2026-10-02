from collections.abc import Sequence
from typing import Protocol

from graphon.model_runtime.v2.application.context import CallContext
from graphon.model_runtime.v2.domain.descriptors import ModelDescriptor
from graphon.model_runtime.v2.domain.identity import ModelRef


class ModelCatalog(Protocol):
    def list_models(self, *, context: CallContext) -> Sequence[ModelDescriptor]: ...

    def describe(self, model: ModelRef, *, context: CallContext) -> ModelDescriptor: ...
