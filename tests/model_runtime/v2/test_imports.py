"""Keep the additive declarations independent of the execution runtime."""

import ast
import importlib.util
import sys
from pathlib import Path

V2_SOURCE_DIRECTORY = Path(__file__).parents[3] / "src/graphon/model_runtime/v2"
V2_PACKAGE = "graphon.model_runtime.v2"


def test_v2_imports_only_inward_and_from_the_standard_library() -> None:
    assert V2_SOURCE_DIRECTORY.is_dir()
    for path in V2_SOURCE_DIRECTORY.rglob("*.py"):
        parts = path.relative_to(V2_SOURCE_DIRECTORY).with_suffix("").parts
        package = ".".join((V2_PACKAGE, *parts[:-1]))
        allowed_import_prefixes = (f"{V2_PACKAGE}.domain",)
        if parts[0] != "domain":
            allowed_import_prefixes += (f"{V2_PACKAGE}.application",)
        if parts == ("__init__",):
            allowed_import_prefixes = (f"{V2_PACKAGE}.application",)
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                imports = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                module = "." * node.level + (node.module or "")
                module = (
                    importlib.util.resolve_name(module, package)
                    if node.level
                    else module
                )
                imports = [f"{module}.{alias.name}" for alias in node.names]
            else:
                continue
            for module in imports:
                assert module.split(".")[0] in sys.stdlib_module_names or any(
                    module == prefix or module.startswith(prefix + ".")
                    for prefix in allowed_import_prefixes
                ), (path, module)
