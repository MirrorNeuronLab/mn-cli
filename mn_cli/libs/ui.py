from rich.console import Group
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from typing import Dict, Any, Optional

def generate_live_layout(job_id: str, data: Dict[str, Any]) -> Group:
    summary = data.get("summary", {})
    job = data.get("job", {})
    agents = data.get("agents", [])
    
    status = summary.get("status", "unknown")
    color = "cyan"
    if status == "completed":
        color = "green"
    elif status in ["failed", "cancelled"]:
        color = "red"
        
    last_event = summary.get("last_event", "N/A")
    
    # Top panel: Job info (Executors removed)
    spinner_str = "[cyan]⠋[/cyan]"
    try:
        import time
        frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
        idx = int(time.time() * 12.5) % len(frames)
        spinner_str = f"[cyan]{frames[idx]}[/cyan]"
    except Exception:
        pass

    info_text = (
        f"[bold]Job ID:[/bold] {job_id}\n"
        f"[bold]Name:[/bold] {job.get('job_name', 'N/A')} | [bold]Graph:[/bold] {job.get('graph_id', 'N/A')}\n"
        f"[bold]Status:[/bold] [{color}]{status}[/{color}] | [bold]Live:[/bold] {summary.get('live?', False)}\n"
        f"[bold]Nodes:[/bold] {len(summary.get('nodes', []))}\n"
        f"[bold]Last Event:[/bold] {last_event} {spinner_str if status not in ['completed', 'failed', 'cancelled'] else ''}\n\n"
        f"[dim]Press 'q' or Ctrl-C to exit[/dim]"
    )
    info_panel = Panel(info_text, title="Live Job Monitor", border_style=color)
    
    # Agents Table
    agent_table = Table(title="Agents (Top 20 by Processed Messages)", expand=True)
    agent_table.add_column("Agent ID")
    agent_table.add_column("Type")
    agent_table.add_column("Status")
    agent_table.add_column("Processed", justify="right")
    agent_table.add_column("Mailbox", justify="right")
    
    sorted_agents = sorted(agents, key=lambda x: x.get("processed_messages", 0), reverse=True)[:20]
    
    for ag in sorted_agents:
        ag_status = ag.get("status", "unknown")
        st_color = "green" if ag_status in ["running", "completed"] else "yellow" if ag_status in ["ready", "busy", "queued"] else "red"
        
        agent_table.add_row(
            ag.get("agent_id", "N/A"),
            ag.get("agent_type", "N/A"),
            f"[{st_color}]{ag_status}[/{st_color}]",
            str(ag.get("processed_messages", 0)),
            str(ag.get("mailbox_depth", 0))
        )
        
    return Group(info_panel, agent_table)

def generate_summary_panel(job_id: str, status: str, log_dir) -> Panel:
    status_text = "Unknown"
    status_color = "yellow"
    if status == "completed":
        status_text = "Success"
        status_color = "green"
    elif status == "failed":
        status_text = "Failed"
        status_color = "red"
    elif status == "cancelled":
        status_text = "Cancelled"
        status_color = "red"
        
    log_file = log_dir / "events.log"
    
    panel_text = (
        f"[bold {status_color}]Job Status: {status_text}[/bold {status_color}]\n\n"
        f"Job ID: {job_id}\n"
        f"Outputs:\n"
        f"  Logs:   {log_file}"
    )
    if (log_dir / "result.txt").exists():
        panel_text += f"\n  Result: {log_dir / 'result.txt'}"
    if (log_dir / "result_stream.txt").exists():
        panel_text += f"\n  Stream: {log_dir / 'result_stream.txt'}"
        
    return Panel(
        panel_text,
        title="Job Execution Summary",
        border_style=status_color,
        expand=False
    )

def generate_run_submitted_panel(
    *,
    bundle_name: str,
    job_id: str,
    payload_count: int,
    log_dir,
    follow_seconds: float,
    run_mode: str = "Batch",
    blueprint_run_id: Optional[str] = None,
    blueprint_revision: Optional[str] = None,
    web_ui_url: Optional[str] = None,
) -> Panel:
    table = Table.grid(padding=(0, 1))
    table.add_column(style="bold")
    table.add_column()
    table.add_row("Bundle", bundle_name)
    table.add_row("Job ID", f"[bold cyan]{job_id}[/bold cyan]")
    if blueprint_run_id:
        table.add_row("Blueprint Run ID", f"[bold green]{blueprint_run_id}[/bold green]")
    if blueprint_revision:
        table.add_row("Blueprint Revision", blueprint_revision[:12])
    table.add_row("Type", run_mode)
    if web_ui_url:
        table.add_row("Web UI", f"[bold green]{web_ui_url}[/bold green]")
    table.add_row("Payloads", str(payload_count))
    table.add_row("Logs", str(log_dir / "events.log"))
    table.add_row("Snapshot", str(log_dir / "job_snapshot.json"))
    table.add_row("Follow", f"{follow_seconds:g}s event tail, then detach")

    return Panel(
        table,
        title="Job submitted successfully",
        border_style="cyan",
        expand=False,
    )

def generate_detached_panel(
    job_id: str,
    log_dir,
    status: str,
    event_count: int,
    *,
    web_ui_url: Optional[str] = None,
) -> Panel:
    status_color = (
        "green"
        if status == "completed"
        else "red"
        if status in {"failed", "cancelled"}
        else "yellow"
    )
    status_label = status.replace("_", " ").title() if status else "Unknown"

    table = Table.grid(padding=(0, 1))
    table.add_column(style="bold")
    table.add_column()
    table.add_row("Status", f"[{status_color}]{status_label}[/{status_color}]")
    table.add_row("Job ID", f"[bold cyan]{job_id}[/bold cyan]")
    table.add_row("Events Logged", str(event_count))
    table.add_row("Raw Events", str(log_dir / "events.log"))
    table.add_row("Run Log", str(log_dir / "run.log"))
    if web_ui_url:
        table.add_row("Web UI", f"[bold green]{web_ui_url}[/bold green]")
    table.add_row("Monitor", f"mn job monitor {job_id}")

    message = Text()
    if status in {"completed", "failed", "cancelled"}:
        message.append("Final job state reached.", style=status_color)
    else:
        message.append("Detached while job is still scheduled/running.", style="yellow")

    return Panel(
        Group(message, table),
        title="Run Detached",
        border_style=status_color,
        expand=False,
    )
