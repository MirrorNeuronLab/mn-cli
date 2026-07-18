"""Public facade for blueprint run commands.

The implementation lives in focused helper modules and command handlers so each
command can be read and tested in isolation. This package preserves the former
``mn_cli.libs.run_cmds`` import surface for callers.
"""

from __future__ import annotations

import sys
import types
from importlib import import_module

_MODULE_NAMES = (
    "common",
    "runtime_dependencies",
    "outputs",
    "model_cluster",
    "model_config",
    "models",
    "context",
    "live",
    "events",
    "openshell",
    "run_state",
    "web_ui",
    "handlers.validate",
    "handlers.doctor",
    "handlers.run",
    "handlers.monitor",
    "handlers.result",
)

_EXPORT_MODULES = [import_module(f"{__name__}.{module_name}") for module_name in _MODULE_NAMES]

for _module in _EXPORT_MODULES:
    for _name in getattr(_module, "__all__", ()):  # re-export legacy surface
        if _name not in {"annotations"}:
            globals()[_name] = getattr(_module, _name)


def _propagate_patch(name: str, value) -> None:
    for module in _EXPORT_MODULES:
        if hasattr(module, name):
            setattr(module, name, value)


class _RunCmdsModule(types.ModuleType):
    def __setattr__(self, name: str, value) -> None:
        super().__setattr__(name, value)
        if not name.startswith("__"):
            _propagate_patch(name, value)


sys.modules[__name__].__class__ = _RunCmdsModule

__all__ = [
    name
    for name in globals()
    if not name.startswith("__") and name not in {"sys", "types", "import_module"}
]
