"""Presentation adapter for shared job statistics."""
import typer
from mn_sdk import RuntimeService
from mn_cli.shared import client, console
from mn_cli.libs.ui import print_detail
from mn_cli.error_handler import handle_cli_error
from mn_cli.output import record_result


def analysis(job_id: str = typer.Argument(help="Durable job ID."),
             json_output: bool = typer.Option(False, "--json", help="Print the shared analysis as JSON.")):
    """Show performance statistics for all recorded job history."""
    try:
        result = RuntimeService(client).analyze_job(job_id)
        if json_output is True:
            console.print_json(data=result)
        else:
            runs, tokens, duration = result["runs"], result["tokens"], result["running_time"]
            def value(number, complete=True):
                return "Unavailable" if number is None else f"{number:,}" + (" (partial)" if not complete else "")
            milliseconds = duration["total_ms"]
            elapsed = "Unavailable" if milliseconds is None else f"{milliseconds / 1000:,.1f} seconds"
            if milliseconds is not None and not duration["complete"]:
                elapsed += " (partial)"
            print_detail(console, "Job performance", {
                "Job": job_id, "Scope": "All recorded history", "Updated": result["snapshot_at"],
                "Total runs": value(runs["total"], result["history_complete"]),
                "Successful": runs["successful"], "Failed": runs["failed"],
                "Success rate": "—" if runs["success_rate"] is None else f'{runs["success_rate"]:.1%}',
                "Cancelled": runs["cancelled"], "Running": runs["running"], "Paused": runs["paused"],
                "Other unfinished": runs["other"], "Running time": elapsed,
                "Tokens used": value(tokens["total_tokens"], tokens["complete"]),
                "Input tokens": value(tokens["input_tokens"], tokens["complete"]),
                "Output tokens": value(tokens["output_tokens"], tokens["complete"]),
                "Estimated tokens": value(tokens["estimated_tokens"], tokens["complete"]),
                "Token coverage": f'{tokens["measured_runs"]}/{tokens["total_runs"]} runs with measurements',
                "Time coverage": f'{duration["measured_runs"]}/{duration["total_runs"]} runs with measurements',
            })
        record_result(result)
    except Exception as exc:
        handle_cli_error(exc, console, "job analysis", command_context={"job_id": job_id})
