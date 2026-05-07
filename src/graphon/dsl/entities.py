from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum, auto
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field

from graphon.entities.graph_config import NodeConfigDict


class DslKind(StrEnum):
    APP = auto()
    RAG_PIPELINE = auto()
    GRAPH = auto()


class LoadStatus(StrEnum):
    LOADABLE = auto()
    UNSUPPORTED = auto()
    FAILED = auto()


class PluginDependencyType(StrEnum):
    GITHUB = auto()
    MARKETPLACE = auto()
    PACKAGE = auto()


class DslDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: DslKind
    graph_config: Mapping[str, Any] | None = None


class DslDependency(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: PluginDependencyType
    plugin_unique_identifier: str | None = None
    repo: str | None = None
    package: str | None = None
    source: Mapping[str, Any] = Field(default_factory=dict)


class DslModelCredential(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider: str | None = None
    vendor: str | None = None
    plugin_unique_identifier: str | None = None
    values: Mapping[str, Any] = Field(default_factory=dict)


class DslToolCredential(BaseModel):
    model_config = ConfigDict(extra="forbid")

    plugin_unique_identifier: str | None = None
    provider_id: str | None = None
    provider: str | None = None
    tool_name: str | None = None
    credential_type: str = "api-key"
    values: Mapping[str, Any] = Field(default_factory=dict)


class DslSlimSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: str = "local"
    plugin_folder: str | None = None
    plugin_root: str | None = None
    plugin_roots: Mapping[str, str] = Field(default_factory=dict)
    daemon_addr: str = ""
    daemon_key: str = ""
    python_path: str = "python3"
    uv_path: str = ""
    python_env_init_timeout: int = 120
    max_execution_timeout: int = 600
    pip_mirror_url: str = ""
    pip_extra_args: str = ""
    marketplace_url: str = "https://marketplace.dify.ai"
    ignore_uv_lock: bool = False


class DslCredentials(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_credentials: list[DslModelCredential] = Field(default_factory=list)
    tool_credentials: list[DslToolCredential] = Field(default_factory=list)
    slim: DslSlimSettings = Field(default_factory=DslSlimSettings)


class DslImportPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    document: DslDocument
    load_status: LoadStatus
    dependencies: list[DslDependency] = Field(default_factory=list)
    load_reason: str | None = None

    @property
    def loadable(self) -> bool:
        return self.load_status == LoadStatus.LOADABLE


class TypedNodeFactory(Protocol):
    def create_node(self, node_config: NodeConfigDict) -> Any: ...
