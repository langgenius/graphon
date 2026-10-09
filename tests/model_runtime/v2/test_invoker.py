import pytest

from graphon.model_runtime.v2 import (
    CallContext,
    ContractRef,
    ModelInvoker,
    ModelRef,
    ModelRequest,
    ModelResult,
)


class ScoreInvoker:
    def invoke(self, request: ModelRequest, *, context: CallContext) -> ModelResult:
        return ModelResult(
            request_id=context.request_id,
            model=request.model,
            contract=request.contract,
            output=0.82,
            usage=False,
        )


def invoke_score(
    invoker: ModelInvoker, request: ModelRequest, context: CallContext
) -> ModelResult:
    return invoker.invoke(request, context=context)


def test_complete_only_implementation_exchanges_a_native_scalar() -> None:
    invoker: ModelInvoker = ScoreInvoker()
    request = ModelRequest(
        model=ModelRef(plugin_id="plugin", provider="provider", model="deployment"),
        contract=ContractRef(id="text-score", revision="1"),
        input="Does this passage answer the question?",
    )
    context = CallContext(connection_id="configured", request_id="request")
    result = invoke_score(invoker, request, context)
    assert result.model == request.model
    assert result.contract == request.contract
    assert result.request_id == context.request_id
    assert result.output == pytest.approx(0.82)
    assert isinstance(result.output, float)
    assert result.usage is False
