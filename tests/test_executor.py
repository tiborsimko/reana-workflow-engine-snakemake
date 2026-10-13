# -*- coding: utf-8 -*-
#
# This file is part of REANA.
# Copyright (C) 2026 CERN.
#
# REANA is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""REANA-Workflow-Engine-Snakemake executor tests."""

import json
import logging
from unittest.mock import Mock, create_autospec, patch

import pytest
from reana_commons.api_client import JobControllerAPIClient
from reana_workflow_engine_snakemake.config import RunStatus
from reana_workflow_engine_snakemake.executor import Executor


class MockJob:
    """Mock job object for testing."""

    def __init__(self, name, wildcards=None):
        self.name = name
        self.wildcards = wildcards if wildcards is not None else {}


def make_shell_job():
    """Build a minimal shell job accepted by Executor.run_job."""
    job = Mock()
    job.name = "calculate"
    job.shellcmd = "echo hello"
    job.is_shell = True
    job.is_run = False
    job.container_img_url = "docker://docker.io/library/ubuntu:24.04"
    job.resources = {}
    job.wildcards = {}
    return job


class TestBuildJobName:
    """Tests for Executor._build_job_name method."""

    def test_job_without_wildcards(self):
        """Test job name when no wildcards are present."""
        job = MockJob(name="calculate")
        assert Executor._build_job_name(job) == "calculate"

    def test_job_with_single_wildcard(self):
        """Test job name with a single wildcard."""
        job = MockJob(name="calculate", wildcards={"mass": "100"})
        assert Executor._build_job_name(job) == "calculate (mass=100)"

    def test_job_with_multiple_wildcards(self):
        """Test job name with multiple wildcards."""
        job = MockJob(name="process", wildcards={"sample": "A", "fileno": "22"})
        result = Executor._build_job_name(job)
        assert "process" in result
        assert "sample=A" in result
        assert "fileno=22" in result


@patch("reana_workflow_engine_snakemake.executor.publish_workflow_start")
def test_run_job_reports_submission_failure(publish_workflow_start):
    """Test that an RJC submission failure marks the job and workflow failed."""
    executor = Executor.__new__(Executor)
    executor.publisher = Mock()
    executor.rjc_api_client = Mock()
    executor.rjc_api_client.submit.side_effect = RuntimeError("image rejected")
    executor.report_job_error = Mock()
    executor.report_job_submission = Mock()
    job = make_shell_job()

    with patch(
        "reana_workflow_engine_snakemake.executor.publish_job_submission"
    ) as publish_job_submission:
        executor.run_job(job)

    executor.report_job_error.assert_called_once()
    executor.report_job_submission.assert_not_called()
    publish_job_submission.assert_not_called()
    executor.publisher.publish_workflow_status.assert_called_once_with(
        "default",
        RunStatus.failed.value,
        message="Job submission failed for calculate: image rejected",
    )


@patch("reana_workflow_engine_snakemake.executor.publish_workflow_start")
def test_run_job_does_not_report_publish_failure_as_submission_failure(
    publish_workflow_start,
):
    """Test that post-submit publication failures remain visible."""
    executor = Executor.__new__(Executor)
    executor.publisher = Mock()
    executor.rjc_api_client = Mock()
    executor.rjc_api_client.submit.return_value = {"job_id": "job-id"}
    executor.report_job_error = Mock()
    executor.report_job_submission = Mock()
    job = make_shell_job()

    with (
        patch(
            "reana_workflow_engine_snakemake.executor.publish_job_submission",
            side_effect=RuntimeError("publisher failed"),
        ),
        pytest.raises(RuntimeError, match="publisher failed"),
    ):
        executor.run_job(job)

    executor.report_job_error.assert_not_called()
    executor.report_job_submission.assert_not_called()


@patch("reana_workflow_engine_snakemake.executor.publish_workflow_start")
def test_run_job_does_not_report_submission_callback_failure(
    publish_workflow_start,
):
    """Test that Snakemake callback failures are not treated as RJC failures."""
    executor = Executor.__new__(Executor)
    executor.publisher = Mock()
    executor.rjc_api_client = Mock()
    executor.rjc_api_client.submit.return_value = {"job_id": "job-id"}
    executor.report_job_error = Mock()
    executor.report_job_submission = Mock(side_effect=RuntimeError("callback failed"))
    job = make_shell_job()

    with pytest.raises(RuntimeError, match="callback failed"):
        executor.run_job(job)

    executor.report_job_error.assert_not_called()


def make_cancel_executor():
    """Build an executor whose job-controller client has the real interface."""
    executor = Executor.__new__(Executor)
    executor.publisher = Mock()
    executor.rjc_api_client = create_autospec(JobControllerAPIClient, instance=True)
    executor.rjc_api_client._client = Mock()
    return executor


def make_active_job(job_id, resources=None):
    """Build an active job as Snakemake passes it to cancel_jobs."""
    return Mock(external_jobid=job_id, job=Mock(resources=resources or {}))


def test_cancel_jobs_publishes_failed_status():
    """Test that cancelled jobs are deleted and a valid status is published."""
    executor = make_cancel_executor()

    executor.cancel_jobs([make_active_job("job-id")])

    executor.rjc_api_client._client.jobs.delete_job.assert_called_once_with(
        job_id="job-id"
    )
    executor.publisher.publish_workflow_status.assert_called_once_with(
        "default",
        RunStatus.failed.value,
        logs="Snakemake is interrupted and all jobs are cancelled.",
    )
    status = executor.publisher.publish_workflow_status.call_args.args[1]
    assert json.dumps({"status": status}) == '{"status": 3}'


def test_cancel_jobs_forwards_compute_backend():
    """Test that a job is deleted with the compute backend it was sent to."""
    executor = make_cancel_executor()

    executor.cancel_jobs(
        [make_active_job("job-id", resources={"compute_backend": "htcondorcern"})]
    )

    executor.rjc_api_client._client.jobs.delete_job.assert_called_once_with(
        job_id="job-id", compute_backend="htcondorcern"
    )


def test_cancel_jobs_matches_job_controller_api():
    """Test that the deletion request matches the job-controller API."""
    executor = make_cancel_executor()
    http_client = Mock()
    executor.rjc_api_client = JobControllerAPIClient(
        "reana-job-controller", http_client=http_client
    )

    executor.cancel_jobs(
        [make_active_job("job-id", resources={"compute_backend": "htcondorcern"})]
    )

    http_client.request.assert_called_once()
    request_params = http_client.request.call_args.args[0]
    assert request_params["method"] == "DELETE"
    assert request_params["url"].endswith("/jobs/job-id/")
    assert request_params["params"] == {"compute_backend": "htcondorcern"}


def test_cancel_jobs_deletion_failure_keeps_original_error(caplog):
    """Test that a failed deletion does not hide the error or stop the others."""
    executor = make_cancel_executor()
    delete_job = executor.rjc_api_client._client.jobs.delete_job
    delete_job.side_effect = [Exception("502 Bad Gateway"), Mock()]

    with caplog.at_level(logging.ERROR):
        try:
            raise PermissionError(13, "Permission denied")
        except PermissionError:
            executor.cancel_jobs([make_active_job("job-1"), make_active_job("job-2")])

    assert [c.kwargs["job_id"] for c in delete_job.call_args_list] == [
        "job-1",
        "job-2",
    ]
    executor.publisher.publish_workflow_status.assert_called_once_with(
        "default",
        RunStatus.failed.value,
        logs=(
            "Snakemake is interrupted and all jobs are cancelled.\n"
            "PermissionError: [Errno 13] Permission denied"
        ),
    )
    assert "Error deleting job with id job-1" in caplog.text


def test_cancel_jobs_reports_error_that_stopped_workflow(caplog):
    """Test that the error handled by Snakemake reaches the workflow logs."""
    executor = make_cancel_executor()

    with caplog.at_level(logging.ERROR):
        try:
            raise PermissionError(13, "Permission denied")
        except PermissionError:
            executor.cancel_jobs([])

    executor.publisher.publish_workflow_status.assert_called_once_with(
        "default",
        RunStatus.failed.value,
        logs=(
            "Snakemake is interrupted and all jobs are cancelled.\n"
            "PermissionError: [Errno 13] Permission denied"
        ),
    )
    assert "Traceback" in caplog.text
    assert "PermissionError: [Errno 13] Permission denied" in caplog.text


def test_cancel_jobs_does_not_report_user_interrupt(caplog):
    """Test that a user interrupt is not reported as an error."""
    executor = make_cancel_executor()

    with caplog.at_level(logging.ERROR):
        try:
            raise KeyboardInterrupt()
        except KeyboardInterrupt:
            executor.cancel_jobs([])

    executor.publisher.publish_workflow_status.assert_called_once_with(
        "default",
        RunStatus.failed.value,
        logs="Snakemake is interrupted and all jobs are cancelled.",
    )
    assert "Traceback" not in caplog.text
