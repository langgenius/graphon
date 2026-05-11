from graphon.dsl import slim
from graphon.dsl.slim.llm import SlimLLM


def test_slim_package_exports_standard_llm_runtime() -> None:
    assert slim.SlimLLM is SlimLLM
    assert "SlimLLM" in slim.__all__
