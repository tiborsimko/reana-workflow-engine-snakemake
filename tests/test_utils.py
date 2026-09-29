# -*- coding: utf-8 -*-
#
# This file is part of REANA.
# Copyright (C) 2026 CERN.
#
# REANA is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Compatibility tests for workflow progress reporting."""

from unittest.mock import Mock

from snakemake.api import SnakemakeApi
from snakemake.executors.dryrun import Executor as DryrunExecutor
from snakemake.settings.types import OutputSettings, ResourceSettings

# Register the REANA executor so dry-run validation uses its settings.
from reana_workflow_engine_snakemake import runner  # noqa: F401
from reana_workflow_engine_snakemake.utils import publish_workflow_start


def test_publish_workflow_start_with_real_dag(tmp_path, monkeypatch):
    """Count pending and finished jobs, excluding rules without a command.

    Exercise Snakemake's real DAG throughout a dry run, guarding the private
    attributes used by the progress publisher and REANA executor validation.
    """
    snakefile = tmp_path / "Snakefile"
    snakefile.write_text(
        'rule all:\n    input: "joined.txt"\n\n'
        'rule make:\n    output: "part.txt"\n    shell: "touch {output}"\n\n'
        'rule join:\n    input: "part.txt"\n    output: "joined.txt"\n'
        '    shell: "cat {input} > {output}"\n'
    )
    publisher = Mock()
    run_job = DryrunExecutor.run_job
    seen_jobs = []

    def report_progress(executor, job):
        publish_workflow_start("workflow-id", publisher, job)
        seen_jobs.append(job)
        run_job(executor, job)

    monkeypatch.setattr(DryrunExecutor, "run_job", report_progress)
    with SnakemakeApi(OutputSettings(dryrun=True)) as api:
        workflow = api.workflow(
            resource_settings=ResourceSettings(nodes=2),
            snakefile=snakefile,
            workdir=tmp_path,
        )
        workflow.dag().execute_workflow(executor="reana", pseudo_executor="dryrun")

    assert {job.name for job in seen_jobs} == {"all", "make", "join"}
    assert publisher.publish_workflow_status.call_count == 3
    for call in publisher.publish_workflow_status.call_args_list:
        assert call.args == ("workflow-id",)
        assert call.kwargs["status"] == 1
        assert call.kwargs["message"]["progress"]["total"] == {
            "total": 2,
            "job_ids": [],
        }
    assert not (tmp_path / "part.txt").exists()
    assert not (tmp_path / "joined.txt").exists()
