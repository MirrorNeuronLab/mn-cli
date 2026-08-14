from __future__ import annotations

from typing import Any

from typer.main import get_command

COMMAND_TREE_START = "<!-- BEGIN GENERATED MN COMMAND TREE -->"
COMMAND_TREE_END = "<!-- END GENERATED MN COMMAND TREE -->"


def command_tree_summary(typer_app: Any) -> str:
    """Render the registered public command tree in declaration order."""
    root = get_command(typer_app)
    rows: list[tuple[str, list[str]]] = []
    for group_name, group in root.commands.items():
        children = getattr(group, "commands", {})
        leaves = [
            name
            for name, command in children.items()
            if not getattr(command, "commands", None)
        ]
        rows.append((group_name, leaves))
        for child_name, command in children.items():
            grandchildren = getattr(command, "commands", None)
            if grandchildren:
                rows.append((f"{group_name} {child_name}", list(grandchildren)))
    width = max(len(label) for label, _commands in rows)
    return "\n".join(
        f"{label:<{width}}  {' '.join(commands)}".rstrip()
        for label, commands in rows
    )


def generated_command_tree_block(typer_app: Any) -> str:
    return "\n".join(
        (
            COMMAND_TREE_START,
            "```text",
            command_tree_summary(typer_app),
            "```",
            COMMAND_TREE_END,
        )
    )


def replace_generated_command_tree(document: str, typer_app: Any) -> str:
    start = document.find(COMMAND_TREE_START)
    end = document.find(COMMAND_TREE_END)
    if start < 0 or end < start:
        raise ValueError("CLI reference is missing generated command-tree markers")
    end += len(COMMAND_TREE_END)
    return document[:start] + generated_command_tree_block(typer_app) + document[end:]
