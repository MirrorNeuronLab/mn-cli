from .common import *


def fetch_and_save_results(job_id: str, data: dict = None):
    log_dir = Path(f"/tmp/mn_{job_id}")
    log_dir.mkdir(parents=True, exist_ok=True)

    if data is None:
        try:
            job_json = client.get_job(job_id)
            data = json.loads(job_json)
        except Exception:
            logger.exception("Failed to fetch job result for %s", job_id)
            return

    job = data.get("job") if isinstance(data.get("job"), dict) else data
    job = job if isinstance(job, dict) else {}
    status = job.get("status")

    # Save final result if completed
    if status == "completed":
        result = _resolve_job_result(job)
        if result:
            with open(log_dir / "result.txt", "w") as f:
                json.dump(result, f, indent=2)

    # Save stream results (progressive)
    stream_events = []

    try:
        full_events = []
        for ev_str in client.stream_events(job_id, follow=False):
            try:
                full_events.append(json.loads(ev_str))
            except Exception:
                logger.exception(
                    "Failed to decode event while saving results for %s", job_id
                )
                pass

        for ev in full_events:
            ev_type = ev.get("type")
            if ev_type not in STANDARD_EVENTS:
                stream_events.append(ev.get("payload", ev))
    except Exception:
        logger.exception("Failed to stream events while saving results for %s", job_id)
        pass

    if stream_events:
        with open(log_dir / "result_stream.txt", "w") as f:
            for se in stream_events:
                f.write(json.dumps(se) + "\n")


def _resolve_job_result(job: dict[str, Any]) -> Any:
    result = job.get("result")
    reference = job.get("result_ref")
    if not is_staged_artifact_ref(reference) and isinstance(result, dict):
        reference = result.get("result_ref")
    if not is_staged_artifact_ref(reference):
        return result
    try:
        resolution_env = dict(os.environ)
        resolution_env.setdefault(
            "MN_HOST_SHARED_STORAGE_ROOT",
            RuntimeConfig.from_env().shared_storage_root,
        )
        return resolve_json_reference(reference, env=resolution_env)
    except StagedArtifactError:
        logger.exception("Failed to resolve staged job result")
        return result


def _is_vc_final_artifact(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    reports = value.get("company_reports") or value.get("companyReports")
    return value.get("type") == "vc_early_heuristic_analysis_reports" or (
        isinstance(reports, list)
        and any(isinstance(item, dict) and item for item in reports)
    )


def _extract_final_artifact(value: Any, depth: int = 0) -> Optional[dict[str, Any]]:
    if depth > 100 or value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        for decoded in _json_values_from_text(text):
            found = _extract_final_artifact(decoded, depth + 1)
            if found:
                return found
        return None
    if isinstance(value, list):
        for item in value:
            found = _extract_final_artifact(item, depth + 1)
            if found:
                return found
        return None
    if not isinstance(value, dict):
        return None
    if _is_vc_final_artifact(value):
        return value
    explicit = value.get("final_artifact") or value.get("finalArtifact")
    if isinstance(explicit, dict) and explicit:
        return explicit
    for key in (
        "result",
        "output",
        "last_message",
        "lastMessage",
        "sandbox",
        "payload",
        "data",
        "logs",
    ):
        found = _extract_final_artifact(value.get(key), depth + 1)
        if found:
            return found
    for item in value.values():
        found = _extract_final_artifact(item, depth + 1)
        if found:
            return found
    return None


def _json_values_from_text(text: str) -> list[Any]:
    decoder = json.JSONDecoder()
    values: list[Any] = []
    starts = [0] if text and text[0] in "{[" else []
    starts.extend(
        index for index, char in enumerate(text) if char in "{[" and index != 0
    )
    for start in starts[:50]:
        try:
            value, _end = decoder.raw_decode(text[start:])
        except Exception:
            continue
        values.append(value)
        if values:
            break
    return values


def _manifest_config(manifest: dict[str, Any]) -> dict[str, Any]:
    for node in manifest_nodes(manifest):
        environment = (node.get("config") or {}).get("environment") or {}
        raw_config = environment.get("MN_BLUEPRINT_CONFIG_JSON")
        if isinstance(raw_config, str) and raw_config.strip():
            try:
                decoded = json.loads(raw_config)
            except Exception:
                continue
            if isinstance(decoded, dict):
                return decoded
    return {}


def _expand_user_output_path(value: str) -> Path:
    text = str(value or "").strip()
    home = (
        os.getenv("MN_OUTPUT_HOME")
        or os.getenv("MN_USER_HOME")
        or os.getenv("OTTERDESK_USER_HOME")
        or str(Path.home())
    )
    if text == "~":
        return Path(home).expanduser()
    if text.startswith("~/") or text.startswith("~\\"):
        return Path(home).expanduser() / text[2:]
    return Path(text).expanduser()


def _configured_output_folder(config: dict[str, Any]) -> Optional[Path]:
    payload = (config.get("inputs") or {}).get("payload") or {}
    outputs = config.get("outputs") or {}
    for value in (
        payload.get("output_folder"),
        outputs.get("folder_path"),
        outputs.get("output_folder"),
    ):
        if isinstance(value, str) and value.strip():
            return _expand_user_output_path(value)
    return None


def _safe_slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(value or "").lower()).strip("-")
    return slug or "company"


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=False) + "\n", encoding="utf-8"
    )


def _render_vc_analysis_markdown(report: dict[str, Any]) -> str:
    lines = [
        f"# {report.get('company_name') or 'Company'}",
        "",
        f"- Composite score: {report.get('composite_score', 'n/a')}",
        f"- Confidence: {report.get('confidence', 'n/a')}",
        "",
        "## Methods",
        "",
    ]
    methods = report.get("methods") if isinstance(report.get("methods"), dict) else {}
    for method_id, method in methods.items():
        if not isinstance(method, dict):
            continue
        summary = method.get("evidence_summary") or {}
        lines.extend(
            [
                f"### {method_id.replace('_', ' ').title()}",
                "",
                f"- Status: {method.get('status', 'unknown')}",
                f"- Score: {method.get('score', 'n/a')}",
                f"- Evidence refs: {', '.join(method.get('evidence_refs') or []) or 'none'}",
                f"- Why: {summary.get('status_reason') or method.get('evidence_summary') or 'No method explanation provided.'}",
                "",
            ]
        )
        missing = method.get("missing_evidence") or []
        if missing:
            lines.append(f"- Missing evidence: {'; '.join(map(str, missing))}")
            lines.append("")
    warnings = report.get("warnings") or []
    if warnings:
        lines.extend(["## Warnings", ""])
        lines.extend(f"- {warning}" for warning in warnings)
        lines.append("")
    return "\n".join(lines)


def _write_vc_final_artifact_outputs(
    final_artifact: dict[str, Any], output_folder: Path
) -> list[dict[str, str]]:
    reports = final_artifact.get("company_reports")
    if not isinstance(reports, list) or not reports:
        return []
    output_files: list[dict[str, str]] = []
    company_index = {
        "blueprint_id": "vc_assistant",
        "report_only": True,
        "generated_at": final_artifact.get("generated_at"),
        "companies": [
            {
                "company_name": report.get("company_name"),
                "company_slug": report.get("company_slug")
                or _safe_slug(report.get("company_name")),
                "composite_score": report.get("composite_score"),
                "confidence": report.get("confidence"),
                "method_count": report.get("method_count"),
            }
            for report in reports
            if isinstance(report, dict)
        ],
    }
    _write_json(output_folder / "final_artifact.json", final_artifact)
    _write_json(output_folder / "company_index.json", company_index)
    output_files.extend(
        [
            {
                "kind": "final_artifact_json",
                "path": str(output_folder / "final_artifact.json"),
            },
            {
                "kind": "company_index_json",
                "path": str(output_folder / "company_index.json"),
            },
        ]
    )
    diagnostic_artifacts = [
        ("action_ledger", "action_ledger_json", "action_ledger.json"),
        ("artifact_quality", "artifact_quality_json", "artifact_quality.json"),
        ("run_health", "run_health_json", "run_health.json"),
    ]
    for artifact_key, kind, filename in diagnostic_artifacts:
        artifact_value = final_artifact.get(artifact_key)
        if isinstance(artifact_value, dict):
            artifact_path = output_folder / filename
            _write_json(artifact_path, artifact_value)
            output_files.append({"kind": kind, "path": str(artifact_path)})
    index_lines = ["# VC Assistant Company Index", ""]
    for company in company_index["companies"]:
        index_lines.append(
            f"- {company.get('company_name')}: score {company.get('composite_score', 'n/a')}, confidence {company.get('confidence', 'n/a')}"
        )
    (output_folder / "company_index.md").write_text(
        "\n".join(index_lines) + "\n", encoding="utf-8"
    )
    (output_folder / "run_summary.md").write_text(
        str(final_artifact.get("executive_summary") or "VC Assistant run completed.")
        + "\n",
        encoding="utf-8",
    )
    output_files.extend(
        [
            {
                "kind": "company_index_markdown",
                "path": str(output_folder / "company_index.md"),
            },
            {
                "kind": "run_summary_markdown",
                "path": str(output_folder / "run_summary.md"),
            },
        ]
    )
    for report in reports:
        if not isinstance(report, dict):
            continue
        slug = report.get("company_slug") or _safe_slug(report.get("company_name"))
        company_dir = output_folder / slug
        _write_json(company_dir / "analysis.json", report)
        (company_dir / "analysis.md").write_text(
            _render_vc_analysis_markdown(report), encoding="utf-8"
        )
        _write_json(company_dir / "method_scores.json", report.get("methods") or {})
        _write_json(company_dir / "evidence.json", report.get("evidence") or [])
        _write_json(company_dir / "warnings.json", report.get("warnings") or [])
        _write_json(
            company_dir / "research_sources.json", report.get("research_sources") or []
        )
        output_files.extend(
            [
                {"kind": "analysis", "path": str(company_dir / "analysis.json")},
                {"kind": "analysis_markdown", "path": str(company_dir / "analysis.md")},
                {
                    "kind": "method_scores",
                    "path": str(company_dir / "method_scores.json"),
                },
            ]
        )
    return output_files


def _materialize_completed_blueprint_outputs(
    log_dir: Path, manifest: dict[str, Any]
) -> None:
    result_path = log_dir / "result.txt"
    if not result_path.exists():
        return
    try:
        result = json.loads(result_path.read_text(encoding="utf-8"))
    except Exception:
        logger.exception(
            "Failed to decode blueprint result for output materialization: %s",
            result_path,
        )
        return
    final_artifact = _extract_final_artifact(result)
    if not final_artifact:
        return
    config = _manifest_config(manifest)
    output_folder = _configured_output_folder(config)
    if output_folder is None:
        return
    try:
        materialized = _write_vc_final_artifact_outputs(final_artifact, output_folder)
        if materialized:
            console.print(
                f"[green]Materialized blueprint outputs:[/green] {output_folder}"
            )
    except Exception:
        logger.exception("Failed to materialize blueprint outputs to %s", output_folder)


def _materialize_shared_storage_outputs(storage: dict[str, Any]) -> bool:
    if not isinstance(storage, dict):
        return False
    result = _sdk_materialize_shared_storage_outputs(storage) or {}
    for warning in result.get("warnings") or []:
        logger.warning("Shared output materialization warning: %s", warning)
    for error in result.get("errors") or []:
        logger.error("Shared output materialization error: %s", error)
    for target in result.get("target_paths") or []:
        console.print(f"[green]Materialized shared outputs:[/green] {target}")
    return bool(result.get("copied"))


__all__ = [name for name in globals() if not name.startswith("__")]
