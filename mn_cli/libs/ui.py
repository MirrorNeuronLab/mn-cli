from rich.console import Group
from rich.table import Table
from rich.panel import Panel
from typing import Dict, Any

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
    except:
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
