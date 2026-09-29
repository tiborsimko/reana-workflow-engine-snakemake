# -*- coding: utf-8 -*-
#
# This file is part of REANA.
# Copyright (C) 2026 CERN.
#
# REANA is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""REANA-Workflow-Engine-Snakemake runner tests."""

import logging
import re
from unittest.mock import Mock, patch

import pytest
from snakemake.api import SnakemakeApi
from snakemake.logging import logger as snakemake_logger
from snakemake.settings.types import OutputSettings
from snakemake_interface_logger_plugins.common import LogEvent

from reana_workflow_engine_snakemake.runner import (
    SnakemakeLoggingFormatter,
    _setup_snakemake_logging,
    run_jobs,
)


class TestSnakemakeLoggingFormatter:
    """Tests for SnakemakeLoggingFormatter."""

    def _make_record(self, msg="hello"):
        """Create a minimal log record."""
        return logging.LogRecord(
            name="snakemake",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=msg,
            args=None,
            exc_info=None,
        )

    def test_empty_body_returns_empty_string(self):
        """Test that empty formatted body is suppressed."""
        formatter = SnakemakeLoggingFormatter(logging.Formatter())
        record = self._make_record("")
        assert record.msg == ""
        with patch.object(formatter, "_snakemake_formatter") as mock_fmt:
            mock_fmt.format.return_value = ""
            assert formatter.format(record) == ""

    def test_none_body_returns_empty_string(self):
        """Test that 'None' formatted body is suppressed."""
        formatter = SnakemakeLoggingFormatter(logging.Formatter())
        record = self._make_record("None")
        assert record.msg == "None"
        with patch.object(formatter, "_snakemake_formatter") as mock_fmt:
            mock_fmt.format.return_value = "None"
            assert formatter.format(record) == ""

    def test_timestamp_is_stripped(self):
        """Test that Snakemake timestamp prefix is removed."""
        formatter = SnakemakeLoggingFormatter(logging.Formatter())
        record = self._make_record()
        with patch.object(formatter, "_snakemake_formatter") as mock_fmt:
            mock_fmt.format.return_value = "[Mon Mar  2 11:19:30 2026]\nDone."
            assert "[Mon Mar" in mock_fmt.format.return_value
            result = formatter.format(record)
            assert "[Mon Mar" not in result
            assert "Done." in result
            assert record.msg == "hello"


class TestSetupSnakemakeLogging:
    """Tests for _setup_snakemake_logging."""

    def test_reuses_configured_handler(self, monkeypatch, caplog):
        """Keep Snakemake's settings, filters and other handlers intact."""
        monkeypatch.setattr(snakemake_logger, "handlers", [])
        monkeypatch.setattr(snakemake_logger, "propagate", True)
        with SnakemakeApi(OutputSettings(nocolor=True, show_failed_logs=True)):
            handler = snakemake_logger.handlers[0]
            filters = handler.filters[:]
            formatter = handler.formatter
            other_handler = logging.NullHandler()
            snakemake_logger.addHandler(other_handler)

            _setup_snakemake_logging()
            _setup_snakemake_logging()

            assert snakemake_logger.propagate is False
            assert snakemake_logger.handlers == [handler, other_handler]
            assert handler.filters == filters
            assert isinstance(handler.formatter, SnakemakeLoggingFormatter)
            assert handler.formatter._snakemake_formatter is formatter
            assert formatter.show_failed_logs is True
            assert "falling back to upstream log formatting" not in caplog.text

    @pytest.mark.parametrize("has_handler", [False, True])
    def test_warns_when_default_handler_is_missing(
        self, monkeypatch, caplog, has_handler
    ):
        """Report missing or renamed handlers without altering their output."""
        handler = logging.StreamHandler()
        handler.name = "RenamedStreamHandler"
        formatter = logging.Formatter()
        handler.setFormatter(formatter)
        handlers = [handler] if has_handler else []
        monkeypatch.setattr(snakemake_logger, "handlers", handlers)
        monkeypatch.setattr(snakemake_logger, "propagate", True)

        with caplog.at_level(logging.WARNING, logger="reana-workflow-engine-snakemake"):
            _setup_snakemake_logging()

        assert snakemake_logger.handlers == handlers
        assert handler.formatter is formatter
        assert caplog.record_tuples == [
            (
                "reana-workflow-engine-snakemake",
                logging.WARNING,
                "Snakemake default stream handler not found; "
                "falling back to upstream log formatting.",
            )
        ]

    @pytest.mark.parametrize("message", ["", None, "None"])
    def test_empty_records_emit_nothing(self, monkeypatch, capsys, message):
        """Suppress empty bodies without forbidding job-block separators."""
        monkeypatch.setattr(snakemake_logger, "handlers", [])
        monkeypatch.setattr(snakemake_logger, "propagate", False)
        monkeypatch.setattr(snakemake_logger, "level", logging.INFO)

        with SnakemakeApi(OutputSettings(nocolor=True)):
            _setup_snakemake_logging()
            snakemake_logger.info(message)

        output = capsys.readouterr()
        assert output.out == ""
        assert output.err == ""

    @pytest.mark.parametrize("printshellcmds", [False, True])
    def test_logging_across_api_contexts(self, capsys, monkeypatch, printshellcmds):
        """Keep structured logs readable across workflow starts and shutdowns.

        Expected bodies deliberately track upstream formatting as a canary for
        logging changes. REANA timestamps each record, not each physical line.
        """
        monkeypatch.setattr(snakemake_logger, "handlers", [])
        monkeypatch.setattr(snakemake_logger, "propagate", False)
        monkeypatch.setattr(snakemake_logger, "level", logging.INFO)

        for _ in range(2):
            with SnakemakeApi(
                OutputSettings(printshellcmds=printshellcmds, nocolor=True)
            ):
                _setup_snakemake_logging()
                snakemake_logger.info(
                    None, extra={"event": LogEvent.PROGRESS, "done": 1, "total": 2}
                )
                snakemake_logger.info("first line\nsecond line")
                # Snakemake may insert a blank line before a job block.
                snakemake_logger.info(
                    None,
                    extra={
                        "event": LogEvent.JOB_INFO,
                        "rule_msg": "make output",
                        "jobid": 1,
                        "reason": "missing output",
                    },
                )
                snakemake_logger.info(
                    None,
                    extra={"event": LogEvent.SHELLCMD, "cmd": "echo upgrade"},
                )

        output = capsys.readouterr().err
        assert output.count("1 of 2 steps (50%) done") == 2
        assert output.count("echo upgrade") == (2 if printshellcmds else 0)
        assert "None" not in output
        prefix = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} \| .*? \| INFO \| "
        bodies = re.findall(prefix + r"(.*(?:\nsecond line)?)$", output, re.MULTILINE)
        assert bodies.count("1 of 2 steps (50%) done") == 2
        assert bodies.count("echo upgrade") == (2 if printshellcmds else 0)
        assert bodies.count("first line\nsecond line") == 2
        assert bodies.count("Job 1: make output") == 2
        assert output.count("Reason: missing output") == 2


@pytest.mark.parametrize("level", [logging.DEBUG, logging.INFO, logging.WARNING])
def test_run_jobs_honours_log_level(tmp_path, monkeypatch, capsys, level):
    """Apply REANA's log level to Snakemake's independent logger."""
    monkeypatch.setattr("reana_workflow_engine_snakemake.runner.REANA_LOG_LEVEL", level)
    monkeypatch.setattr(snakemake_logger, "handlers", [])
    monkeypatch.setattr(snakemake_logger, "propagate", True)
    monkeypatch.setattr(snakemake_logger, "level", logging.INFO)

    def workflow(**kwargs):
        snakemake_logger.debug("debug marker")
        snakemake_logger.info("info marker")
        snakemake_logger.warning("warning marker")
        return Mock()

    with (
        patch.object(SnakemakeApi, "workflow", side_effect=workflow),
        patch("reana_workflow_engine_snakemake.runner._generate_report"),
    ):
        assert run_jobs(str(tmp_path), "Snakefile", {})

    output = capsys.readouterr().err
    assert ("debug marker" in output) == (level <= logging.DEBUG)
    assert ("info marker" in output) == (level <= logging.INFO)
    assert "warning marker" in output
