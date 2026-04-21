# Model Runtime

This module provides the interfaces for invoking and authenticating various
models, and offers Dify a unified information and credentials form rule for
model providers.

- On one hand, it decouples models from upstream and downstream processes, facilitating horizontal expansion for developers,
- On the other hand, it allows for direct display of providers and models in the frontend interface by simply defining them in the backend, eliminating the need to modify frontend logic.

## Features

- Supports capability invocation for 6 types of models

  - `LLM` - LLM text completion, dialogue, pre-computed tokens capability
  - `Text Embedding Model` - Text Embedding, pre-computed tokens capability
  - `Rerank Model` - Segment Rerank capability
  - `Speech-to-text Model` - Speech to text capability
  - `Text-to-speech Model` - Text to speech capability
  - `Moderation` - Moderation capability

- Model provider display

  Displays a list of all supported providers, including provider names, icons, supported model types list, predefined model list, configuration method, and credentials form rules, etc.

- Selectable model list display

  After configuring provider/model credentials, the dropdown (application orchestration interface/default model) allows viewing of the available LLM list. Greyed out items represent predefined model lists from providers without configured credentials, facilitating user review of supported models.

  In addition, this list also returns configurable parameter information and rules for LLM. These parameters are all defined in the backend, allowing different settings for various parameters supported by different models.

- Provider/model credential authentication

  The provider list returns configuration information for the credentials form, which can be authenticated through Runtime's interface.

## Structure

Model Runtime is divided into protocol and implementation layers:

- Provider/runtime protocols

  Shared provider concerns live in `protocols/provider_runtime.py`, while each
  model capability has its own protocol module such as
  `protocols/llm_runtime.py`, `protocols/text_embedding_runtime.py`, and
  `protocols/tts_runtime.py`. Downstream runtimes can implement only the
  capabilities they need instead of satisfying a single monolithic interface.

- Aggregate runtime protocol

  `protocols/runtime.py` composes the individual capability protocols into
  `ModelRuntime` for adapters that intentionally implement the full surface
  area.

- Provider factory

  `model_providers/model_provider_factory.py` now depends only on
  `ModelProviderRuntime`. It handles provider discovery, provider/model schema
  lookup, credential validation, provider icon lookup, and provider-level model
  list projection without assuming any invocation capability.

- Model wrappers

  Capability wrappers such as `LargeLanguageModel`, `TextEmbeddingModel`,
  `RerankModel`, `Speech2TextModel`, `ModerationModel`, and `TTSModel` depend
  only on their matching capability protocol. Instantiate those wrappers
  directly when you need invocation behavior.

## Documentation

For detailed documentation on how to add new providers or models, please refer to the [Dify documentation](https://docs.dify.ai/).
