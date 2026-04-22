import typer
import json
from pathlib import Path
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from mn_cli.libs.ui import generate_live_layout, generate_summary_panel
from mn_cli.shared import console, client

STANDARD_EVENTS = {
    "init", "job_pending", "job_validated", "job_scheduled", "job_running",
    "job_completed", "job_failed", "job_paused", "job_resumed", "job_cancelled",
    "agent_recovery_started", "agent_recovered",
    "agent_message_received", "aggregator_received", "aggregator_duplicate_ignored",
    "executor_lease_requested", "executor_lease_acquired", "executor_lease_released",
    "sandbox_job_started", "sandbox_job_completed", "sandbox_job_failed",
    "node_up", "node_down"
}

def fetch_and_save_results(job_id: str, data: dict = None):
    log_dir = Path(f"/tmp/mn_{job_id}")
    log_dir.mkdir(parents=True, exist_ok=True)
    
    if data is None:
        try:
            job_json = client.get_job(job_id)
            data = json.loads(job_json)
        except Exception:
            return

    job = data.get("job", {})
    status = job.get("status")
    
    # Save final result if completed
    if status == "completed":
        result = job.get("result")
        if result:
            with open(log_dir / "result.txt", "w") as f:
                json.dump(result, f, indent=2)
                
    # Save stream results (progressive)
    stream_events = []
    
    try:
        full_events = []
        for ev_str in client.stream_events(job_id):
            try:
                full_events.append(json.loads(ev_str))
            except Exception:
                pass
        
        for ev in full_events:
            ev_type = ev.get("type")
            if ev_type not in STANDARD_EVENTS:
                stream_events.append(ev.get("payload", ev))
    except Exception:
        pass
        
    if stream_events:
        with open(log_dir / "result_stream.txt", "w") as f:
            for se in stream_events:
                f.write(json.dumps(se) + "\n")


def _stream_and_format_events(job_id: str):
    console.print(f"Monitoring events for [bold cyan]{job_id}[/bold cyan]... (Press Ctrl+C to detach)")
    
    log_dir = Path(f"/tmp/mn_{job_id}")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "events.log"
    
    console.print(f"Saving raw logs to: [bold]{log_file}[/bold]\n")
    
    status_text = "Unknown / Detached"
    status_color = "yellow"
    msg_count = 0
    current_step = "[cyan]Connecting..."
    
    try:
        with open(log_file, "a") as f:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                console=console,
            ) as progress:
                job_task = progress.add_task("[cyan]Initializing...", total=4)
                
                for event_json in client.stream_events(job_id):
                    f.write(event_json + "\n")
                    f.flush()
                    try:
                        event = json.loads(event_json)
                        event_type = event.get("type")
                        
                        if event_type not in STANDARD_EVENTS:
                            with open(log_dir / "result_stream.txt", "a") as f_stream:
                                f_stream.write(json.dumps(event.get("payload", event)) + "\n")
                                
                        if event_type == "job_pending":
                            current_step = "[cyan]Pending..."
                            progress.update(job_task, completed=1, description=current_step)
                        elif event_type == "job_validated":
                            current_step = "[cyan]Validated..."
                            progress.update(job_task, completed=2, description=current_step)
                        elif event_type == "job_scheduled":
                            current_step = "[green]Scheduled... (Job detached in background)"
                            progress.update(job_task, completed=4, description=current_step)
                        elif event_type == "job_running":
                            current_step = "[cyan]Running..."
                            progress.update(job_task, completed=4, description=current_step)
                        elif event_type in ["agent_message_received", "aggregator_received"]:
                            msg_count += 1
                            current_step = f"[cyan]Running (Processed {msg_count} msgs)..."
                            progress.update(job_task, completed=4, description=current_step)
                        elif event_type == "job_completed":
                            result = event.get("result")
                            if result is not None:
                                with open(log_dir / "result.txt", "w") as f_res:
                                    json.dump(result, f_res, indent=2)
                                    
                            current_step = "[green]Completed successfully!"
                            progress.update(job_task, completed=4, description=current_step)
                            status_text = "Success"
                            status_color = "green"
                            break
                        elif event_type == "job_failed":
                            current_step = "[red]Job failed!"
                            progress.update(job_task, completed=4, description=current_step)
                            status_text = "Failed"
                            status_color = "red"
                            break
                        else:
                            current_step = f"[cyan]Running ({event_type})..."
                            progress.update(job_task, completed=4, description=current_step)
                    except Exception:
                        pass
                        
        if status_text in ["Success", "Failed"]:
            panel = generate_summary_panel(
                job_id=job_id,
                status="completed" if status_text == "Success" else "failed",
                log_dir=log_dir
            )
            console.print(panel)
        else:
            if current_step.startswith("[green]Scheduled"):
                console.print(f"\n[green]Job successfully detached in background. It is now scheduled/running.[/green]")
            else:
                console.print(f"\n[yellow]Job stream completed prematurely. Last step: {current_step}[/yellow]")
            console.print(f"To monitor again, run: [bold]mn monitor {job_id}[/bold]")
        
    except KeyboardInterrupt:
        console.print(
            f"\n[yellow]Detached from log stream. Job {job_id} is still running.[/yellow]"
        )
        console.print(f"To monitor again, run: [bold]mn monitor {job_id}[/bold]")


def validate(bundle_path: str):
    """Check if a job bundle in a local folder is valid to run"""
    try:
        bundle_dir = Path(bundle_path)
        if not bundle_dir.is_dir():
            console.print(
                f"[red]Error: '{bundle_path}' is not a directory. Expected a bundle folder.[/red]"
            )
            raise typer.Exit(1)

        manifest_file = bundle_dir / "manifest.json"
        if not manifest_file.exists():
            console.print(
                f"[red]Error: manifest.json not found in '{bundle_path}'[/red]"
            )
            raise typer.Exit(1)

        with open(manifest_file, "r") as f:
            try:
                manifest = json.load(f)
            except json.JSONDecodeError as e:
                console.print(f"[red]Error: manifest.json is not valid JSON. {e}[/red]")
                raise typer.Exit(1)

        required_keys = ["manifest_version", "graph_id", "job_name", "entrypoints", "nodes"]
        missing = [k for k in required_keys if k not in manifest]
        if missing:
            console.print(f"[red]Error: manifest.json is missing required keys: {', '.join(missing)}[/red]")
            raise typer.Exit(1)

        if not isinstance(manifest.get("nodes"), type([])):
            console.print("[red]Error: 'nodes' must be a list in manifest.json[/red]")
            raise typer.Exit(1)

        console.print(f"[green]✓ Job bundle at '{bundle_path}' is valid.[/green]")
        console.print(f"  - Job Name: {manifest.get('job_name')}")
        console.print(f"  - Graph ID: {manifest.get('graph_id')}")
        console.print(f"  - Nodes count: {len(manifest.get('nodes'))}")
        
    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"[red]Validation failed: {e}[/red]")
        raise typer.Exit(1)


def run(bundle_path: str):
    """Run a job bundle from a local folder directly"""
    try:
        bundle_dir = Path(bundle_path)
        if not bundle_dir.is_dir():
            console.print(
                f"[red]Error: '{bundle_path}' is not a directory. Expected a bundle folder.[/red]"
            )
            raise typer.Exit(1)

        manifest_file = bundle_dir / "manifest.json"
        if not manifest_file.exists():
            console.print(
                f"[red]Error: manifest.json not found in '{bundle_path}'[/red]"
            )
            raise typer.Exit(1)

        with open(manifest_file, "r") as f:
            manifest = f.read()

        payloads = {}
        payloads_dir = bundle_dir / "payloads"
        if payloads_dir.is_dir():
            for filepath in payloads_dir.rglob("*"):
                if filepath.is_file():
                    rel_path = filepath.relative_to(payloads_dir).as_posix()
                    with open(filepath, "rb") as f:
                        payloads[rel_path] = f.read()

        console.print(
            f"Submitting bundle '{bundle_dir.name}' with {len(payloads)} payloads..."
        )
        job_id = client.submit_job(manifest, payloads)
        console.print(f"[green]Job submitted successfully. Job ID: {job_id}[/green]")
        _stream_and_format_events(job_id)
    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"[red]Error running bundle: {e}[/red]")
        raise typer.Exit(1)


def _live_monitor(job_id: str):
    import sys
    import select
    import time
    from rich.live import Live
    
    is_tty = sys.stdin.isatty()
    old_settings = None
    if is_tty:
        import tty
        import termios
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        tty.setcbreak(fd)
        
    class MonitorView:
        def __init__(self):
            self.data = None
        def __rich__(self):
            if not self.data:
                from rich.panel import Panel
                return Panel("Connecting...", style="cyan")
            if "error" in self.data:
                from rich.panel import Panel
                return Panel(f"Error fetching job: {self.data['error']}", style="red")
            return generate_live_layout(job_id, self.data)

    final_status = "unknown"
    view = MonitorView()
    
    try:
        with Live(view, refresh_per_second=12, console=console) as live:
            while True:
                try:
                    job_json = client.get_job(job_id)
                    data = json.loads(job_json)
                except Exception as e:
                    data = {"error": str(e)}
                    
                view.data = data
                
                if data and "error" not in data:
                    status = data.get("summary", {}).get("status", "unknown")
                    if status in ["completed", "failed", "cancelled"]:
                        final_status = status
                        break
                
                if is_tty:
                    i, o, e = select.select([sys.stdin], [], [], 0.5)
                    if i:
                        key = sys.stdin.read(1)
                        if key.lower() == 'q' or key == '\x03': # \x03 is Ctrl-C
                            break
                else:
                    time.sleep(0.5)
                    break
                    
    except KeyboardInterrupt:
        pass
    finally:
        if is_tty and old_settings:
            import termios
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
            
    if final_status in ["completed", "failed", "cancelled"]:
        # Save results and print final summary
        fetch_and_save_results(job_id, data)
        log_dir = Path(f"/tmp/mn_{job_id}")
        panel = generate_summary_panel(job_id, final_status, log_dir)
        console.print(panel)
    else:
        console.print(f"\n[yellow]Exited live monitor for {job_id}[/yellow]")
        fetch_and_save_results(job_id, data)


def monitor(job_id: str):
    """Stream live events for a job"""
    try:
        _live_monitor(job_id)
    except Exception as e:
        console.print(f"[red]Error streaming events: {e}[/red]")


def result(job_id: str):
    """Fetch and save the final and progressive results for a job"""
    try:
        console.print(f"Fetching results for {job_id}...")
        fetch_and_save_results(job_id)
        
        log_dir = Path(f"/tmp/mn_{job_id}")
        res_file = log_dir / "result.txt"
        stream_file = log_dir / "result_stream.txt"
        
        if res_file.exists():
            console.print(f"[green]Final result saved to: {res_file}[/green]")
        else:
            console.print(f"[yellow]No final result found (job might not be completed).[/yellow]")
            
        if stream_file.exists():
            console.print(f"[green]Stream results saved to: {stream_file}[/green]")
            
    except Exception as e:
        console.print(f"[red]Error fetching results: {e}[/red]")
