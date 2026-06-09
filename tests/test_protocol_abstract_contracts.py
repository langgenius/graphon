from __future__ import annotations

import ast
import importlib
import inspect
from pathlib import Path

import pytest


def _is_direct_protocol_class(
    class_def: ast.ClassDef,
    *,
    protocol_aliases: set[str],
) -> bool:
    for base in class_def.bases:
        base_expr = base.value if isinstance(base, ast.Subscript) else base
        if isinstance(base_expr, ast.Name) and base_expr.id in protocol_aliases:
            return True
        if (
            isinstance(base_expr, ast.Attribute)
            and isinstance(base_expr.value, ast.Name)
            and f"{base_expr.value.id}.{base_expr.attr}" in protocol_aliases
        ):
            return True
    return False


def _discover_protocol_aliases(parsed: ast.Module) -> set[str]:
    protocol_aliases = set[str]()
    typing_aliases: set[str] = set()

    for node in parsed.body:
        if isinstance(node, ast.ImportFrom) and node.module in {
            "typing",
            "typing_extensions",
        }:
            for alias in node.names:
                if alias.name == "Protocol":
                    protocol_aliases.add(alias.asname or alias.name)
            continue

        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in {"typing", "typing_extensions"}:
                    typing_aliases.add(alias.asname or alias.name)

    protocol_aliases.update(f"{alias}.Protocol" for alias in typing_aliases)
    return protocol_aliases


def _has_protocol_members(class_def: ast.ClassDef) -> bool:
    return any(
        isinstance(member, ast.FunctionDef | ast.AsyncFunctionDef)
        for member in class_def.body
    )


def _discover_protocol_targets() -> list[type[object]]:
    src_root = Path(__file__).resolve().parents[1] / "src" / "graphon"
    protocol_classes: list[type[object]] = []

    for file_path in sorted(src_root.rglob("*.py")):
        parsed = ast.parse(file_path.read_text())
        protocol_aliases = _discover_protocol_aliases(parsed)
        if not protocol_aliases:
            continue

        module_name = "graphon." + ".".join(
            file_path.relative_to(src_root).with_suffix("").parts,
        )
        module = importlib.import_module(module_name)

        for class_def in [
            node for node in parsed.body if isinstance(node, ast.ClassDef)
        ]:
            if not _is_direct_protocol_class(
                class_def,
                protocol_aliases=protocol_aliases,
            ):
                continue
            if not _has_protocol_members(class_def):
                continue
            protocol_classes.append(getattr(module, class_def.name))

    protocol_classes.sort(key=lambda cls: (cls.__module__, cls.__qualname__))
    return protocol_classes


def _protocol_member_names(protocol_cls: type[object]) -> list[str]:
    member_names: list[str] = []
    for name, value in protocol_cls.__dict__.items():
        if name.startswith("__") and name.endswith("__"):
            continue
        if isinstance(value, property | classmethod | staticmethod):
            member_names.append(name)
            continue
        if inspect.isfunction(value):
            member_names.append(name)
    return member_names


PROTOCOL_TARGETS = _discover_protocol_targets()


def _protocol_id(protocol_cls: type[object]) -> str:
    return f"{protocol_cls.__module__}.{protocol_cls.__qualname__}"


def test_protocol_targets_should_be_discovered() -> None:
    assert PROTOCOL_TARGETS


@pytest.mark.parametrize(
    "protocol_cls",
    PROTOCOL_TARGETS,
    ids=_protocol_id,
)
def test_protocol_members_should_be_abstract(protocol_cls: type[object]) -> None:
    member_names = _protocol_member_names(protocol_cls)
    # This protects the test from a vacuous pass if discovery and runtime member
    # detection drift apart. Discovery only targets Protocol classes with
    # methods, so an empty member list means the test is no longer checking the
    # contract it claims to check.
    assert member_names, f"{_protocol_id(protocol_cls)} has no protocol members."

    non_abstract_members = [
        name
        for name in member_names
        if not getattr(protocol_cls.__dict__[name], "__isabstractmethod__", False)
    ]
    assert non_abstract_members == []
