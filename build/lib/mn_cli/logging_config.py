from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from mn_cli.config import load_config


def configure_logging(
    name: str,
    log_path: Path,
    *,
    level: str | None = None,
) -> logging.Logger:
    config = load_config(app_name="mn-cli")
    logger = logging.getLogger(name)
    logger.setLevel((level or str(config.get("MN_LOG_LEVEL", "info"))).upper())
    logger.propagate = False

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )

    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handler: logging.Handler = RotatingFileHandler(
            log_path,
            maxBytes=int(config.get("MN_LOG_MAX_BYTES", 1048576)),
            backupCount=int(config.get("MN_LOG_BACKUP_COUNT", 5)),
        )
    except OSError:
        handler = logging.StreamHandler()

    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
