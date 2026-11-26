from ..common import *
from ..outputs import *

def result(job_id: str):
    """Fetch and save the final and progressive results for a job"""
    try:
        console.print(f"Fetching results for {job_id}...")
        fetch_and_save_results(job_id)

        log_dir = Path(f"/tmp/mn_{job_id}")
        res_file = log_dir / "result.txt"
        stream_file = log_dir / "result_stream.txt"

        details: list[tuple[str, Path]] = []
        if res_file.exists():
            details.append(("Final result", res_file))
        else:
            console.print(
                "[yellow]No final result found (job might not be completed).[/yellow]"
            )

        if stream_file.exists():
            details.append(("Stream results", stream_file))

        if details:
            print_success_confirmation(
                console,
                "Job result fetch",
                details=[("Job ID", job_id), *details],
            )

    except Exception as e:
        handle_cli_error(e, console, "fetch results")


__all__ = [name for name in globals() if not name.startswith("__")]
