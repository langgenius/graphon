from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

from graphon.dsl.slim.config import SlimConfig, SlimLocalSettings, SlimProviderBinding
from graphon.dsl.slim.package_loader import SlimPackageLoader
from graphon.model_runtime.entities.model_entities import ModelFeature


def test_slim_package_loader_selects_requested_provider(tmp_path: Path) -> None:
    plugin_root = tmp_path / "plugin"
    _write_multi_provider_plugin(plugin_root)

    loader = SlimPackageLoader(
        SlimConfig(
            bindings=[
                SlimProviderBinding(
                    plugin_id="author/fake:0.0.1@test",
                    provider="other-provider",
                    plugin_root=plugin_root,
                ),
            ],
            local=SlimLocalSettings(folder=tmp_path / "plugins"),
        ),
    )

    loaded = loader.load(
        SlimProviderBinding(
            plugin_id="author/fake:0.0.1@test",
            provider="other-provider",
            plugin_root=plugin_root,
        ),
    )

    assert loaded.provider_entity.provider == "other-provider"
    assert loaded.provider_entity.models[0].model == "other-chat"
    assert ModelFeature.POLLING in (loaded.provider_entity.models[0].features or [])


def test_slim_config_auto_discovers_uv_and_python(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "graphon.dsl.slim.config.shutil.which",
        lambda name: "/usr/local/bin/uv" if name == "uv" else None,
    )

    config = SlimConfig(
        bindings=[SlimProviderBinding(plugin_id="author/fake:0.0.1@test")],
        local=SlimLocalSettings(folder=tmp_path / "plugins"),
    )

    assert config.local.folder == (tmp_path / "plugins").resolve()
    assert config.local.python_path == sys.executable
    assert config.local.uv_path == "/usr/local/bin/uv"


@pytest.mark.parametrize(
    ("raw_features", "infer_from_rule", "expected_features"),
    [
        (None, False, None),
        ([], True, []),
        (None, True, [ModelFeature.STRUCTURED_OUTPUT]),
        (
            ["structured-output"],
            False,
            [ModelFeature.STRUCTURED_OUTPUT],
        ),
    ],
    ids=["unknown", "unsupported", "inferred", "supported"],
)
def test_slim_package_loader_preserves_model_feature_tri_state(
    tmp_path: Path,
    raw_features: list[str] | None,
    infer_from_rule: bool,
    expected_features: list[ModelFeature] | None,
) -> None:
    loader = SlimPackageLoader(
        SlimConfig(
            bindings=[SlimProviderBinding(plugin_id="author/fake:0.0.1@test")],
            local=SlimLocalSettings(folder=tmp_path),
        ),
    )
    raw_model = {
        "model": "chat-model",
        "label": {"en_US": "Chat Model"},
        "model_type": "llm",
        "fetch_from": "predefined-model",
        "model_properties": {},
        "parameter_rules": (
            [{"name": "json_schema", "type": "string"}] if infer_from_rule else []
        ),
    }
    if raw_features is not None:
        raw_model["features"] = raw_features

    model = loader.convert_model_entity(raw_model)

    assert model is not None
    assert model.features == expected_features


def _write_multi_provider_plugin(plugin_root: Path) -> None:
    (plugin_root / "_assets").mkdir(parents=True, exist_ok=True)
    (plugin_root / "provider").mkdir(parents=True, exist_ok=True)
    (plugin_root / "models" / "llm").mkdir(parents=True, exist_ok=True)

    (plugin_root / "manifest.yaml").write_text(
        textwrap.dedent(
            """
            plugins:
              models:
                - provider/first.yaml
                - provider/second.yaml
            """,
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    (plugin_root / "_assets" / "icon.svg").write_text(
        "<svg xmlns='http://www.w3.org/2000/svg'></svg>\n",
        encoding="utf-8",
    )

    for provider_name, label, provider_file, model_file, model_name in (
        (
            "fake-provider",
            "Fake Provider",
            "provider/first.yaml",
            "models/llm/fake-chat.yaml",
            "fake-chat",
        ),
        (
            "other-provider",
            "Other Provider",
            "provider/second.yaml",
            "models/llm/other-chat.yaml",
            "other-chat",
        ),
    ):
        (plugin_root / provider_file).write_text(
            textwrap.dedent(
                f"""
                provider: {provider_name}
                label:
                  en_US: {label}
                description:
                  en_US: Provider for tests.
                icon_small:
                  en_US: icon.svg
                supported_model_types:
                  - llm
                configurate_methods:
                  - predefined-model
                models:
                  llm:
                    predefined:
                      - {model_file}
                """,
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        (plugin_root / model_file).write_text(
            textwrap.dedent(
                f"""
                model: {model_name}
                label:
                  en_US: {label} Model
                model_type: llm
                fetch_from: predefined-model
                features:
                  - polling
                model_properties:
                  mode: chat
                  context_size: 8192
                parameter_rules:
                  - name: temperature
                    use_template: temperature
                """,
            ).strip()
            + "\n",
            encoding="utf-8",
        )


def test_slim_package_loader_retains_credential_help(tmp_path: Path) -> None:
    """The slim loader must carry a manifest's per-field `help` through.

    `_convert_credential_form_schema` builds `CredentialFormSchema` field by
    field, so adding the field to the entity alone is not enough on this path —
    this test covers that second drop point.
    """
    plugin_root = tmp_path / "plugin"
    _write_credential_help_plugin(plugin_root)

    binding = SlimProviderBinding(
        plugin_id="author/fake:0.0.1@test",
        provider="help-provider",
        plugin_root=plugin_root,
    )
    loader = SlimPackageLoader(
        SlimConfig(
            bindings=[binding],
            local=SlimLocalSettings(folder=tmp_path / "plugins"),
        ),
    )

    loaded = loader.load(binding)
    credential_schema = loaded.provider_entity.provider_credential_schema
    assert credential_schema is not None
    form_schemas = {
        schema.variable: schema for schema in credential_schema.credential_form_schemas
    }

    assert form_schemas["api_key"].help is not None
    assert form_schemas["api_key"].help.en_us == "Find this in the provider console."
    assert form_schemas["api_key"].help.zh_hans == "在提供商控制台中查找。"
    # A field without `help` still loads, unchanged.
    assert form_schemas["api_base"].help is None


def _write_credential_help_plugin(plugin_root: Path) -> None:
    (plugin_root / "_assets").mkdir(parents=True, exist_ok=True)
    (plugin_root / "provider").mkdir(parents=True, exist_ok=True)
    (plugin_root / "models" / "llm").mkdir(parents=True, exist_ok=True)

    (plugin_root / "manifest.yaml").write_text(
        textwrap.dedent(
            """
            plugins:
              models:
                - provider/help.yaml
            """,
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    (plugin_root / "_assets" / "icon.svg").write_text(
        "<svg xmlns='http://www.w3.org/2000/svg'></svg>\n",
        encoding="utf-8",
    )
    (plugin_root / "provider" / "help.yaml").write_text(
        textwrap.dedent(
            """
            provider: help-provider
            label:
              en_US: Help Provider
            icon_small:
              en_US: icon.svg
            supported_model_types:
              - llm
            configurate_methods:
              - predefined-model
            models:
              llm:
                predefined:
                  - models/llm/help-chat.yaml
            provider_credential_schema:
              credential_form_schemas:
                - variable: api_key
                  label:
                    en_US: API Key
                  type: secret-input
                  required: true
                  help:
                    en_US: Find this in the provider console.
                    zh_Hans: 在提供商控制台中查找。
                - variable: api_base
                  label:
                    en_US: API Base
                  type: text-input
                  required: false
            """,
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    (plugin_root / "models" / "llm" / "help-chat.yaml").write_text(
        textwrap.dedent(
            """
            model: help-chat
            label:
              en_US: Help Chat Model
            model_type: llm
            fetch_from: predefined-model
            model_properties:
              mode: chat
              context_size: 8192
            """,
        ).strip()
        + "\n",
        encoding="utf-8",
    )
