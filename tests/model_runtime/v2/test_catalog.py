from collections.abc import Sequence

from graphon.model_runtime.v2 import (
    CallContext,
    ContractRef,
    DataFormat,
    ModelCatalog,
    ModelContract,
    ModelDescriptor,
    ModelRef,
)


class ConfiguredCatalog:
    def list_models(self, *, context: CallContext) -> Sequence[ModelDescriptor]:
        del context
        return ()

    def describe(self, model: ModelRef, *, context: CallContext) -> ModelDescriptor:
        return ModelDescriptor(
            ref=model,
            contracts=(
                ModelContract(
                    ref=ContractRef(id="text-score", revision="1"),
                    request=DataFormat(
                        schema={"type": "object"}, mime_types=("text/plain",)
                    ),
                    output=DataFormat(
                        schema={"type": "number"}, mime_types=("application/json",)
                    ),
                    delivery=frozenset({"complete"}),
                    stream=None,
                    accepts_output_schema=False,
                ),
            ),
            operation_tags=(),
            label=context.connection_id,
        )


def describe_configured_model(
    catalog: ModelCatalog, model: ModelRef, context: CallContext
) -> ModelDescriptor:
    models: Sequence[ModelDescriptor] = catalog.list_models(context=context)
    assert models == ()
    return catalog.describe(model, context=context)


def test_catalog_only_implementation_can_describe_an_unlisted_configured_model() -> (
    None
):
    catalog: ModelCatalog = ConfiguredCatalog()
    context = CallContext(connection_id="configured", request_id="discovery")
    model = ModelRef(plugin_id="plugin", provider="provider", model="deployment")
    descriptor = describe_configured_model(catalog, model, context)
    assert descriptor.ref == model
    assert descriptor.label == context.connection_id
    assert descriptor.contracts[0].ref == ContractRef(id="text-score", revision="1")
