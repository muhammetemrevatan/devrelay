import ast
import sys
from importlib.util import resolve_name
from pathlib import Path

import devrelay.domain


def test_domain_imports_only_itself_and_the_standard_library() -> None:
    root = Path(devrelay.domain.__file__).parent
    violations: list[str] = []
    for file in root.rglob("*.py"):
        relative = file.relative_to(root)
        package = ".".join(("devrelay", "domain", *relative.parts[:-1]))
        for node in ast.walk(ast.parse(file.read_text(encoding="utf-8"))):
            modules: list[str] = []
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if node.level:
                    module = resolve_name("." * node.level + module, package)
                modules = [module]
            else:
                continue
            for module in modules:
                if module == "devrelay.domain" or module.startswith("devrelay.domain."):
                    continue
                if module.split(".")[0] not in sys.stdlib_module_names:
                    violations.append(f"{relative}:{node.lineno}: {module}")
    assert not violations, "Forbidden domain imports: " + "; ".join(violations)
