from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

from graphon.dsl.slim.config import SlimConfig, SlimLocalSettings, SlimProviderBinding
from graphon.dsl.slim.package_loader import SlimPackageLoader


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
