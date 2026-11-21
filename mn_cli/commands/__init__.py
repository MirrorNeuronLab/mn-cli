"""Register each public command family and expose its historical callbacks."""

import typer

from . import (
    blueprint,
    job,
    model,
    node,
    operation,
    resource,
    run,
    runtime,
    schedule,
    service,
)
from .blueprint import (
    blueprint_add as blueprint_add,
    blueprint_cleanup as blueprint_cleanup,
    blueprint_doctor as blueprint_doctor,
    blueprint_export as blueprint_export,
    blueprint_list as blueprint_list,
    blueprint_options as blueprint_options,
    blueprint_remove as blueprint_remove,
    blueprint_run as blueprint_run,
    blueprint_show as blueprint_show,
    blueprint_update as blueprint_update,
    blueprint_validate as blueprint_validate,
)
from .job import (
    job_analysis as job_analysis,
    job_archive as job_archive,
    job_backup as job_backup,
    job_create as job_create,
    job_delete as job_delete,
    job_list as job_list,
    job_relocate as job_relocate,
    job_reset_data as job_reset_data,
    job_restore as job_restore,
    job_show as job_show,
    job_start as job_start,
    mailbox_ack as mailbox_ack,
    mailbox_list as mailbox_list,
    mailbox_send as mailbox_send,
)
from .model import (
    model_add as model_add,
    model_doctor as model_doctor,
    model_list as model_list,
    model_probe as model_probe,
    model_remove as model_remove,
    model_show as model_show,
    model_start as model_start,
    model_stop as model_stop,
    model_unload as model_unload,
    model_update as model_update,
)
from .node import (
    node_add as node_add,
    node_drain as node_drain,
    node_list as node_list,
    node_maintenance as node_maintenance,
    node_reconcile as node_reconcile,
    node_refresh_token as node_refresh_token,
    node_remove as node_remove,
    node_show as node_show,
    node_undrain as node_undrain,
)
from .operation import (
    operation_show as operation_show,
    operation_watch as operation_watch,
)
from .resource import (
    resource_set as resource_set,
    resource_show as resource_show,
    resource_usage as resource_usage,
)
from .run import (
    run_cancel as run_cancel,
    run_compare as run_compare,
    run_delete as run_delete,
    run_human_ack as run_human_ack,
    run_human_list as run_human_list,
    run_human_respond as run_human_respond,
    run_info as run_info,
    run_list as run_list,
    run_logs as run_logs,
    run_pause as run_pause,
    run_resources as run_resources,
    run_result as run_result,
    run_resume as run_resume,
    run_retry as run_retry,
    run_show as run_show,
    run_watch as run_watch,
)
from .runtime import (
    runtime_cleanup as runtime_cleanup,
    runtime_doctor as runtime_doctor,
    runtime_ensure_context_engine as runtime_ensure_context_engine,
    runtime_reconnect as runtime_reconnect,
    runtime_restart_sidecars as runtime_restart_sidecars,
    runtime_start as runtime_start,
    runtime_status as runtime_status,
    runtime_stop as runtime_stop,
    runtime_upgrade as runtime_upgrade,
)
from .schedule import (
    schedule_cancel as schedule_cancel,
    schedule_list as schedule_list,
    schedule_set_time as schedule_set_time,
    schedule_show as schedule_show,
)
from .service import (
    service_list as service_list,
    service_show as service_show,
)


def register_commands(app: typer.Typer) -> None:
    blueprint.register(app)
    job.register(app)
    schedule.register(app)
    run.register(app)
    model.register(app)
    runtime.register(app)
    node.register(app)
    operation.register(app)
    resource.register(app)
    service.register(app)
