"""Common text formats for LLM plugins and consumers."""

from graphon.model_runtime.v2.domain.descriptors import ModelContract
from graphon.model_runtime.v2.domain.formats import DataFormat
from graphon.model_runtime.v2.domain.identity import ContractRef

LLM_CONTRACT = ModelContract(
    ref=ContractRef(id="llm", revision="1"),
    request=DataFormat(
        profile="graphon.llm.request/1",
        kinds=("text",),
        schema={
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "required": ["input", "parameters"],
            "additionalProperties": False,
            "properties": {
                "input": {
                    "type": "object",
                    "required": ["messages"],
                    "properties": {
                        "messages": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "required": ["role", "content"],
                                "properties": {
                                    "role": {"type": "string"},
                                    "content": {"type": "string"},
                                },
                            },
                        },
                    },
                },
                "parameters": {"type": "object"},
            },
        },
    ),
    output=DataFormat(
        profile="graphon.llm.output/1",
        kinds=("text",),
        schema={
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "required": ["text"],
            "properties": {"text": {"type": "string"}},
        },
    ),
    delivery=frozenset({"complete"}),
    stream=DataFormat(
        profile="graphon.llm.stream/1",
        kinds=("text",),
        schema={
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "required": ["text_delta"],
            "properties": {"text_delta": {"type": "string"}},
        },
    ),
)
