from graphon import protocols
from graphon.file.protocols import WorkflowFileRuntimeProtocol
from graphon.graph.graph import NodeFactory
from graphon.graph.validation import GraphValidationRule
from graphon.http.protocols import HttpClientProtocol, HttpResponseProtocol
from graphon.model_runtime.memory.prompt_message_memory import PromptMessageMemory
from graphon.model_runtime.protocols.llm_runtime import LLMModelRuntime
from graphon.model_runtime.protocols.moderation_runtime import (
    ModerationModelRuntime,
)
from graphon.model_runtime.protocols.provider_runtime import ModelProviderRuntime
from graphon.model_runtime.protocols.rerank_runtime import RerankModelRuntime
from graphon.model_runtime.protocols.runtime import ModelRuntime
from graphon.model_runtime.protocols.speech_to_text_runtime import (
    SpeechToTextModelRuntime,
)
from graphon.model_runtime.protocols.text_embedding_runtime import (
    TextEmbeddingModelRuntime,
)
from graphon.model_runtime.protocols.tts_runtime import TTSModelRuntime
from graphon.nodes.code.code_node import WorkflowCodeExecutor
from graphon.nodes.llm.protocols import CredentialsProvider, ModelFactory
from graphon.nodes.llm.runtime_protocols import (
    LLMProtocol,
    PromptMessageSerializerProtocol,
    RetrieverAttachmentLoaderProtocol,
)
from graphon.nodes.protocols import (
    FileManagerProtocol,
    FileReferenceFactoryProtocol,
    ToolFileManagerProtocol,
)
from graphon.nodes.runtime import (
    HumanInputFormStateProtocol,
    HumanInputNodeRuntimeProtocol,
    ToolNodeRuntimeProtocol,
)
from graphon.protocols import (
    CredentialsProvider as PublicCredentialsProvider,
)
from graphon.protocols import (
    FileManagerProtocol as PublicFileManagerProtocol,
)
from graphon.protocols import (
    FileReferenceFactoryProtocol as PublicFileReferenceFactoryProtocol,
)
from graphon.protocols import GraphValidationRule as PublicGraphValidationRule
from graphon.protocols import HttpClientProtocol as PublicHttpClientProtocol
from graphon.protocols import HttpResponseProtocol as PublicHttpResponseProtocol
from graphon.protocols import (
    HumanInputFormStateProtocol as PublicHumanInputFormStateProtocol,
)
from graphon.protocols import (
    HumanInputNodeRuntimeProtocol as PublicHumanInputNodeRuntimeProtocol,
)
from graphon.protocols import LLMModelRuntime as PublicLLMModelRuntime
from graphon.protocols import LLMProtocol as PublicLLMProtocol
from graphon.protocols import ModelFactory as PublicModelFactory
from graphon.protocols import ModelProviderRuntime as PublicModelProviderRuntime
from graphon.protocols import ModelRuntime as PublicModelRuntime
from graphon.protocols import ModerationModelRuntime as PublicModerationModelRuntime
from graphon.protocols import NodeFactory as PublicNodeFactory
from graphon.protocols import PromptMessageMemory as PublicPromptMessageMemory
from graphon.protocols import (
    PromptMessageSerializerProtocol as PublicPromptMessageSerializerProtocol,
)
from graphon.protocols import (
    ReadOnlyGraphRuntimeState as PublicReadOnlyGraphRuntimeState,
)
from graphon.protocols import ReadOnlyVariablePool as PublicReadOnlyVariablePool
from graphon.protocols import RerankModelRuntime as PublicRerankModelRuntime
from graphon.protocols import (
    RetrieverAttachmentLoaderProtocol as PublicRetrieverAttachmentLoaderProtocol,
)
from graphon.protocols import (
    SpeechToTextModelRuntime as PublicSpeechToTextModelRuntime,
)
from graphon.protocols import (
    TextEmbeddingModelRuntime as PublicTextEmbeddingModelRuntime,
)
from graphon.protocols import ToolFileManagerProtocol as PublicToolFileManagerProtocol
from graphon.protocols import ToolNodeRuntimeProtocol as PublicToolNodeRuntimeProtocol
from graphon.protocols import TTSModelRuntime as PublicTTSModelRuntime
from graphon.protocols import VariableLoader as PublicVariableLoader
from graphon.protocols import WorkflowCodeExecutor as PublicWorkflowCodeExecutor
from graphon.protocols import (
    WorkflowFileRuntimeProtocol as PublicWorkflowFileRuntimeProtocol,
)
from graphon.runtime.graph_runtime_state_protocol import (
    ReadOnlyGraphRuntimeState,
    ReadOnlyVariablePool,
)
from graphon.variable_loader import VariableLoader


def test_public_protocol_exports_match_canonical_definitions() -> None:
    assert PublicHttpClientProtocol is HttpClientProtocol
    assert PublicHttpResponseProtocol is HttpResponseProtocol
    assert PublicWorkflowFileRuntimeProtocol is WorkflowFileRuntimeProtocol
    assert PublicNodeFactory is NodeFactory
    assert PublicGraphValidationRule is GraphValidationRule
    assert PublicModelProviderRuntime is ModelProviderRuntime
    assert PublicLLMModelRuntime is LLMModelRuntime
    assert PublicTextEmbeddingModelRuntime is TextEmbeddingModelRuntime
    assert PublicRerankModelRuntime is RerankModelRuntime
    assert PublicSpeechToTextModelRuntime is SpeechToTextModelRuntime
    assert PublicModerationModelRuntime is ModerationModelRuntime
    assert PublicTTSModelRuntime is TTSModelRuntime
    assert PublicModelRuntime is ModelRuntime
    assert PublicPromptMessageMemory is PromptMessageMemory
    assert PublicWorkflowCodeExecutor is WorkflowCodeExecutor
    assert PublicCredentialsProvider is CredentialsProvider
    assert PublicModelFactory is ModelFactory
    assert PublicLLMProtocol is LLMProtocol
    assert PublicPromptMessageSerializerProtocol is PromptMessageSerializerProtocol
    assert PublicRetrieverAttachmentLoaderProtocol is RetrieverAttachmentLoaderProtocol
    assert PublicFileManagerProtocol is FileManagerProtocol
    assert PublicToolFileManagerProtocol is ToolFileManagerProtocol
    assert PublicFileReferenceFactoryProtocol is FileReferenceFactoryProtocol
    assert PublicToolNodeRuntimeProtocol is ToolNodeRuntimeProtocol
    assert PublicHumanInputNodeRuntimeProtocol is HumanInputNodeRuntimeProtocol
    assert PublicHumanInputFormStateProtocol is HumanInputFormStateProtocol
    assert PublicReadOnlyVariablePool is ReadOnlyVariablePool
    assert PublicReadOnlyGraphRuntimeState is ReadOnlyGraphRuntimeState
    assert PublicVariableLoader is VariableLoader


def test_public_protocol_package_exports_are_stable() -> None:
    assert protocols.__all__ == [
        "CredentialsProvider",
        "FileManagerProtocol",
        "FileReferenceFactoryProtocol",
        "GraphValidationRule",
        "HttpClientProtocol",
        "HttpResponseProtocol",
        "HumanInputFormStateProtocol",
        "HumanInputNodeRuntimeProtocol",
        "LLMModelRuntime",
        "LLMProtocol",
        "ModelFactory",
        "ModelProviderRuntime",
        "ModelRuntime",
        "ModerationModelRuntime",
        "NodeFactory",
        "PromptMessageMemory",
        "PromptMessageSerializerProtocol",
        "ReadOnlyGraphRuntimeState",
        "ReadOnlyVariablePool",
        "RerankModelRuntime",
        "RetrieverAttachmentLoaderProtocol",
        "SpeechToTextModelRuntime",
        "TTSModelRuntime",
        "TextEmbeddingModelRuntime",
        "ToolFileManagerProtocol",
        "ToolNodeRuntimeProtocol",
        "VariableLoader",
        "WorkflowCodeExecutor",
        "WorkflowFileRuntimeProtocol",
    ]
