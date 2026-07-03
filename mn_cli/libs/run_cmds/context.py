from .common import *

def _ensure_context_engine_for_run_if_needed(
    bundle_dir: Path,
    manifest: dict[str, Any],
    *,
    env_overrides: Optional[dict[str, str]] = None,
    config_overrides: Optional[dict[str, Any]] = None,
    force: bool = False,
) -> dict[str, str] | None:
    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides)
    effective_env = os.environ.copy()
    effective_env.update(
        {
            str(key): str(value)
            for key, value in (env_overrides or {}).items()
            if value is not None
        }
    )
    if not blueprint_requires_context_engine(manifest, config, env=effective_env):
        return None

    console.print(f"[cyan]{CONTEXT_ENGINE_EXPECTATION}[/cyan]")
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
        disable=not use_progress(),
    ) as progress:
        task = progress.add_task(
            "[cyan]Preparing context memory: checking Membrane and Docker Model Runner...",
            total=None,
        )
        summary = ensure_context_engine_runtime(force=force)
        progress.update(task, description="[green]Context memory is ready.")
    console.print(
        f"[green]Context memory ready:[/green] {summary.get('service', 'membrane-context-engine')} "
        f"using {summary.get('model', 'configured model')}"
    )
    logger.info("Context engine runtime ensured for %s: %s", bundle_dir, summary)
    return summary


__all__ = [name for name in globals() if not name.startswith("__")]
