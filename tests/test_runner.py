# -*- coding: utf-8 -*-
#
# This file is part of REANA.
# Copyright (C) 2026 CERN.
#
# REANA is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""REANA-Workflow-Engine-Snakemake runner tests."""

import logging
from unittest.mock import patch

from reana_workflow_engine_snakemake.runner import (
    SnakemakeLoggingFormatter,
    _setup_snakemake_logging,
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
        formatter = SnakemakeLoggingFormatter()
        record = self._make_record("")
        assert record.msg == ""
        with patch.object(formatter, "_snakemake_formatter") as mock_fmt:
            mock_fmt.format.return_value = ""
            assert formatter.format(record) == ""

    def test_none_body_returns_empty_string(self):
        """Test that 'None' formatted body is suppressed."""
        formatter = SnakemakeLoggingFormatter()
        record = self._make_record("None")
        assert record.msg == "None"
        with patch.object(formatter, "_snakemake_formatter") as mock_fmt:
            mock_fmt.format.return_value = "None"
            assert formatter.format(record) == ""

    def test_timestamp_is_stripped(self):
        """Test that Snakemake timestamp prefix is removed."""
        formatter = SnakemakeLoggingFormatter()
        record = self._make_record()
        with patch.object(formatter, "_snakemake_formatter") as mock_fmt:
            mock_fmt.format.return_value = "[Mon Mar  2 11:19:30 2026]\nDone."
            assert "[Mon Mar" in mock_fmt.format.return_value
            result = formatter.format(record)
            assert "[Mon Mar" not in result
            assert "Done." in result


class TestSetupSnakemakeLogging:
    """Tests for _setup_snakemake_logging."""

    def test_replaces_handlers(self):
        """Test that existing handlers are replaced with a single REANA one."""
        from snakemake.logging import logger as snakemake_logger

        old_handlers = snakemake_logger.handlers[:]
        old_propagate = snakemake_logger.propagate
        try:
            # Ensure multiple handlers exist; the logger may start empty
            snakemake_logger.addHandler(logging.StreamHandler())
            snakemake_logger.addHandler(logging.StreamHandler())
            assert len(snakemake_logger.handlers) >= 2
            assert not isinstance(
                snakemake_logger.handlers[0].formatter, SnakemakeLoggingFormatter
            )
            _setup_snakemake_logging()
            assert snakemake_logger.propagate is False
            assert len(snakemake_logger.handlers) == 1
            assert isinstance(
                snakemake_logger.handlers[0].formatter, SnakemakeLoggingFormatter
            )
        finally:
            snakemake_logger.handlers = old_handlers
            snakemake_logger.propagate = old_propagate
