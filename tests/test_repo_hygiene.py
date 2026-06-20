from pathlib import Path


def test_mn_cli_does_not_import_blueprint_support_skill():
    root = Path(__file__).resolve().parents[1] / "mn_cli"
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        forbidden_import = "from mn_blueprint_support" in text or "import mn_blueprint_support" in text
        forbidden_bootstrap = "_inject_local_blueprint_support_path" in text or "_ensure_blueprint_support_path" in text
        forbidden_path = "mn-skills/blueprint_support_skill/src" in text
        if forbidden_import or forbidden_bootstrap or forbidden_path:
            offenders.append(str(path.relative_to(root.parent)))
    assert offenders == []


def test_mn_cli_blueprint_support_skill_mentions_are_runtime_staging_only():
    root = Path(__file__).resolve().parents[1] / "mn_cli"
    allowed = {Path("libs/run_manifest.py")}
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if "blueprint_support_skill" in text and path.relative_to(root) not in allowed:
            offenders.append(str(path.relative_to(root.parent)))
    assert offenders == []
