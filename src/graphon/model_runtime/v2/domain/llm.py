"""Shared LLM message, output, and preview formats."""

from graphon.model_runtime.v2.domain.descriptors import ModelContract
from graphon.model_runtime.v2.domain.formats import DataFormat
from graphon.model_runtime.v2.domain.identity import ContractRef
from graphon.model_runtime.v2.domain.json_values import JsonObject


def _build_content_definitions() -> JsonObject:
    return {
        "media_source": {
            "oneOf": [
                {
                    "type": "object",
                    "required": ["type", "uri"],
                    "properties": {
                        "type": {"const": "uri"},
                        "uri": {"type": "string", "minLength": 1},
                    },
                    "additionalProperties": False,
                },
                {
                    "type": "object",
                    "required": ["type", "id"],
                    "properties": {
                        "type": {"const": "file"},
                        "id": {"type": "string", "minLength": 1},
                    },
                    "additionalProperties": False,
                },
                {
                    "type": "object",
                    "required": ["type", "encoding", "data"],
                    "properties": {
                        "type": {"const": "inline"},
                        "encoding": {"const": "base64"},
                        "data": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
            ]
        },
        "text_part": {
            "type": "object",
            "required": ["type", "text"],
            "properties": {"type": {"const": "text"}, "text": {"type": "string"}},
        },
        "media_part": {
            "type": "object",
            "required": ["type", "mime_type", "source"],
            "properties": {
                "type": {"enum": ["image", "audio", "video", "document"]},
                "mime_type": {"type": "string", "minLength": 1},
                "source": {"$ref": "#/$defs/media_source"},
            },
        },
        "content_part": {
            "oneOf": [
                {"$ref": "#/$defs/text_part"},
                {"$ref": "#/$defs/media_part"},
            ]
        },
    }


LLM_CONTRACT = ModelContract(
    ref=ContractRef(id="llm", revision="1"),
    request=DataFormat(
        profile="graphon.llm.request/1",
        kinds=("text", "image", "audio", "video", "document"),
        schema={
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": _build_content_definitions(),
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
                                    "content": {
                                        "oneOf": [
                                            {"type": "string"},
                                            {
                                                "type": "array",
                                                "items": {
                                                    "$ref": "#/$defs/content_part"
                                                },
                                            },
                                        ]
                                    },
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
        kinds=("text", "image", "audio", "video", "document"),
        schema={
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": _build_content_definitions(),
            "type": "object",
            "anyOf": [{"required": ["text"]}, {"required": ["content"]}],
            "properties": {
                "text": {"type": "string"},
                "content": {
                    "type": "array",
                    "items": {"$ref": "#/$defs/content_part"},
                },
            },
        },
    ),
    delivery=frozenset({"complete"}),
    stream=DataFormat(
        profile="graphon.llm.stream/1",
        kinds=("text", "image", "audio", "video", "document"),
        schema={
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": _build_content_definitions(),
            "type": "object",
            "oneOf": [
                {
                    "required": ["text_delta"],
                    "not": {"required": ["type"]},
                    "properties": {"text_delta": {"type": "string"}},
                },
                {
                    "required": ["type", "index", "text"],
                    "properties": {
                        "type": {"const": "text_delta"},
                        "index": {"type": "integer", "minimum": 0},
                        "text": {"type": "string"},
                    },
                },
                {
                    "required": ["type", "index", "part"],
                    "properties": {
                        "type": {"const": "content_part"},
                        "index": {"type": "integer", "minimum": 0},
                        "part": {"$ref": "#/$defs/content_part"},
                    },
                },
            ],
        },
    ),
)
