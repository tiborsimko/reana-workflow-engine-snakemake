# -*- coding: utf-8 -*-
#
# This file is part of REANA.
# Copyright (C) 2021, 2022, 2023, 2024, 2025, 2026 CERN.
#
# REANA is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""REANA-Workflow-Engine-Snakemake runner."""

import os
import logging
import re
from copy import copy
from pathlib import Path

from snakemake.api import SnakemakeApi
from snakemake.logging import logger as snakemake_logger
from snakemake.settings.types import (
    ConfigSettings,
    DAGSettings,
    DeploymentSettings,
    OutputSettings,
    ResourceSettings,
    StorageSettings,
    WorkflowSettings,
)

from snakemake_interface_executor_plugins.registry import ExecutorPluginRegistry
from snakemake_interface_report_plugins.registry import ReportPluginRegistry
from snakemake_interface_report_plugins.settings import (
    ReportSettingsBase,
)
from snakemake_interface_common.exceptions import WorkflowError

from reana_commons.config import REANA_LOG_FORMAT, REANA_LOG_LEVEL

from reana_workflow_engine_snakemake.config import (
    LOGGING_MODULE,
    SNAKEMAKE_MAX_PARALLEL_JOBS,
    DEFAULT_SNAKEMAKE_REPORT_FILENAME,
)

from reana_workflow_engine_snakemake import executor as reana_executor

log = logging.getLogger(LOGGING_MODULE)


class SnakemakeLoggingFormatter(logging.Formatter):
    """Format Snakemake log records for REANA output.

    Delegates to Snakemake's configured formatter to produce human-readable
    message bodies (e.g. PROGRESS → "2 of 5 steps (40%) done"), then wraps
    the result in ``REANA_LOG_FORMAT``.  Records whose formatted body is
    empty or ``"None"`` are returned as empty strings, which Snakemake's
    stream handler suppresses.
    """

    _SNAKEMAKE_TIMESTAMP_RE = re.compile(r"^\[.*?\]\n")

    def __init__(self, snakemake_formatter):
        """Initialise Snakemake formatter with REANA log format."""
        super().__init__(fmt=REANA_LOG_FORMAT)
        self._snakemake_formatter = snakemake_formatter

    def format(self, record):
        """Format a log record."""
        # Other handlers, including Snakemake's file logger, need the original
        # structured record rather than REANA's rendered message.
        record = copy(record)
        body = self._snakemake_formatter.format(record)
        if not body or body == "None":
            return ""
        # Strip Snakemake's own timestamp (e.g. "[Mon Mar  2 11:19:30 2026]\n")
        # since REANA_LOG_FORMAT already provides one.
        body = self._SNAKEMAKE_TIMESTAMP_RE.sub("", body)
        record.msg = body
        record.args = None
        return super().format(record)


def _setup_snakemake_logging():
    """Wrap Snakemake's default stream formatter with REANA formatting.

    This must be called **after** ``SnakemakeApi(...)`` has been created,
    because the ``SnakemakeApi`` constructor triggers ``LoggerManager`` setup
    which installs Snakemake's default ``ColorizingTextHandler``.

    Reuse the handler, filter and formatter configured from ``OutputSettings``
    so their constructor signatures and defaults remain Snakemake's concern.
    Disable propagation to prevent duplicate root-logger messages. Snakemake
    may subsequently add its usual workspace log file when creating a workflow.
    """
    snakemake_logger.propagate = False

    for handler in snakemake_logger.handlers:
        if handler.name == "DefaultStreamHandler" and not isinstance(
            handler.formatter, SnakemakeLoggingFormatter
        ):
            handler.setFormatter(SnakemakeLoggingFormatter(handler.formatter))

    if not any(
        isinstance(handler.formatter, SnakemakeLoggingFormatter)
        for handler in snakemake_logger.handlers
    ):
        log.warning(
            "Snakemake default stream handler not found; "
            "falling back to upstream log formatting."
        )


my_registry = ExecutorPluginRegistry()
my_registry.register_plugin("reana", reana_executor)


def _generate_report(dag_api, workflow_workspace, report_file_name):
    """Generate HTML report."""
    from snakemake.report import html_reporter

    registry = ReportPluginRegistry()
    registry.register_plugin("html", html_reporter)
    report_plugin = registry.get_plugin("html")

    report_args = ReportSettingsBase()
    report_args.report_html_path = Path(workflow_workspace) / report_file_name
    report_args.report_html_stylesheet_path = None

    report_settings = report_plugin.get_settings(args=report_args)
    dag_api.create_report(reporter="html", report_settings=report_settings)


def run_jobs(
    workflow_workspace,
    workflow_file,
    workflow_parameters,
    operational_options={},
):
    """Run Snakemake jobs using custom REANA executor."""
    workflow_file_path = os.path.join(workflow_workspace, workflow_file)
    with SnakemakeApi(
        OutputSettings(
            printshellcmds=True,
            nocolor=True,
            verbose=REANA_LOG_LEVEL <= logging.DEBUG,
            log_level_override=REANA_LOG_LEVEL,
        )
    ) as snakemake_api:
        _setup_snakemake_logging()
        try:
            workflow_api = snakemake_api.workflow(
                resource_settings=ResourceSettings(nodes=SNAKEMAKE_MAX_PARALLEL_JOBS),
                config_settings=ConfigSettings(config=workflow_parameters),
                storage_settings=StorageSettings(),
                storage_provider_settings=dict(),
                workflow_settings=WorkflowSettings(),
                deployment_settings=DeploymentSettings(),
                snakefile=Path(workflow_file_path),
                workdir=Path(workflow_workspace),
            )
            dag_api = workflow_api.dag(
                dag_settings=DAGSettings(),
            )

            dag_api.execute_workflow(
                executor="reana",
            )

            report_file_name = operational_options.get(
                "report", DEFAULT_SNAKEMAKE_REPORT_FILENAME
            )
            _generate_report(dag_api, workflow_workspace, report_file_name)
            return True

        except WorkflowError as e:
            snakemake_api.print_exception(e)
            return False
