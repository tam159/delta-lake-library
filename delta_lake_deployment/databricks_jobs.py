"""Databricks jobs."""

from typing import Dict, List, Optional, Tuple

from databricks_client import DatabricksClient
from job_config import (
    CLUSTER_ID,
    FAILURE_ALERT_EMAILS,
    STREAM_EXTRACT_RAW_EVENT_PYSPARK,
    STREAM_INGEST_KAFKA_PYSPARK,
)

from delta_lake_library.helpers import StringConversion


class DatabricksJobs(DatabricksClient):
    """Databricks jobs client."""

    def creat_batch_job(
        self,
        job_name: str,
        python_file: str,
        timeout_seconds: int = 0,
        max_retries: int = 0,
        quartz_cron_expression: Optional[str] = None,
        parameters: Optional[List[str]] = None,
        max_concurrent_runs: int = 1,
    ) -> None:
        """
        Create batch job.

        :param job_name: job name
        :param python_file: python file path in dbfs
        :param timeout_seconds: timeout seconds. Default no timeout
        :param max_retries: max retries. Default no retry
        :param quartz_cron_expression: cron expression e.g. "0 0 12 ? * Sun". Default None
        :param parameters: parameters. Default None
        :param max_concurrent_runs: max concurrent runs. Default 1
        :return: None
        """
        schedule: Optional[Dict[str, str]] = None

        if quartz_cron_expression:
            schedule = {
                "quartz_cron_expression": quartz_cron_expression,
                "timezone_id": "UTC",
            }

        self.jobs.create_job(
            name=job_name,
            existing_cluster_id=CLUSTER_ID,
            email_notifications={
                "on_failure": FAILURE_ALERT_EMAILS,
                "no_alert_for_skipped_runs": False,
            },
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            schedule=schedule,
            spark_python_task={
                "python_file": python_file,
                "parameters": parameters,
            },
            max_concurrent_runs=max_concurrent_runs,
        )

    def get_job_last_parameters(self, job_name_prefix: str) -> List[str]:
        """
        Get existing job topics.

        :param job_name_prefix: job name prefix e.g. stream_ingest_
        :return: List of last parameter
        """
        jobs = self.jobs.list_jobs()

        if len(jobs) == 0:
            last_parameters = []
        else:
            last_parameters = [
                job["settings"]["spark_python_task"]["parameters"][-1]
                for job in jobs["jobs"]
                if job["settings"]["name"].startswith(job_name_prefix)
            ]

        return last_parameters

    def creat_stream_job(
        self,
        job_name: str,
        python_file: str,
        parameters: List[str],
        max_retries: int = 5,
        max_concurrent_runs: int = 1,
    ) -> None:
        """
        Create stream job.

        :param job_name: job name
        :param python_file: python file path in dbfs
        :param parameters: parameters e.g. ["--data_type", "event"]
        :param max_retries: max retries. Default 5
        :param max_concurrent_runs: max concurrent runs. Default 1
        :return: None
        """
        self.jobs.create_job(
            name=job_name,
            existing_cluster_id=CLUSTER_ID,
            email_notifications={
                "on_failure": FAILURE_ALERT_EMAILS,
                "no_alert_for_skipped_runs": False,
            },
            max_retries=max_retries,
            spark_python_task={
                "python_file": python_file,
                "parameters": parameters,
            },
            max_concurrent_runs=max_concurrent_runs,
        )

    def creat_stream_ingest_jobs(self, data_type: str, topics: Tuple[str]) -> None:
        """
        Create streaming ingestion jobs.

        :param data_type: data type (e.g. event, cdc)
        :param topics: topics name
        :return: None
        """
        existing_job_topics = self.get_job_last_parameters("stream_ingest_")

        for topic in topics:
            if topic not in existing_job_topics:
                topic_snake_case_name = StringConversion.camel_to_snake(topic)
                job_name = f"stream_ingest_{topic_snake_case_name}"
                parameters = ["--data_type", data_type, "--topic_name", topic]

                self.creat_stream_job(
                    job_name=job_name,
                    python_file=STREAM_INGEST_KAFKA_PYSPARK,
                    parameters=parameters,
                )

    def creat_stream_extract_raw_event_jobs(self, tables: Tuple[str]) -> None:
        """
        Create streaming extraction raw event jobs.

        :param tables: table names
        :return: None
        """
        existing_job_tables = self.get_job_last_parameters("stream_extract_raw_")

        for table in tables:
            if table not in existing_job_tables:
                job_name = f"stream_extract_raw_{table}"
                parameters = ["--table_name", table]

                self.creat_stream_job(
                    job_name=job_name,
                    python_file=STREAM_EXTRACT_RAW_EVENT_PYSPARK,
                    parameters=parameters,
                )

    def get_active_runs(self, run_name_prefix: Optional[str] = None) -> List[int]:
        """
        Get active jobs runs.

        :param run_name_prefix: run name prefix e.g. stream_ingest_
        :return: List of active run ids
        """
        runs = self.jobs.list_runs(active_only=True)

        if "runs" in runs:
            if run_name_prefix:
                run_ids = [
                    run["run_id"]
                    for run in runs["runs"]
                    if run["run_name"].startswith(run_name_prefix)
                ]
            else:
                run_ids = [run["run_id"] for run in runs["runs"]]
        else:
            run_ids = []

        return run_ids

    def cancel_runs(self, run_name_prefix: Optional[str] = None) -> None:
        """
        Cancel active jobs runs.

        :param run_name_prefix: run name prefix e.g. stream_ingest_
        :return: None
        """
        run_ids = self.get_active_runs(run_name_prefix)
        for run_id in run_ids:
            self.jobs.cancel_run(run_id=run_id)

    def get_jobs(self, job_name_prefix: Optional[str] = None) -> List[int]:
        """
        Get job ids.

        :param job_name_prefix: job name prefix e.g. stream_ingest_
        :return: List of job ids
        """
        jobs = self.jobs.list_jobs()

        if len(jobs) == 0:
            job_ids = []
        else:
            if job_name_prefix:
                job_ids = [
                    job["job_id"]
                    for job in jobs["jobs"]
                    if job["settings"]["name"].startswith(job_name_prefix)
                ]
            else:
                job_ids = [job["job_id"] for job in jobs["jobs"]]

        return job_ids

    def get_active_jobs(self, job_name_prefix: Optional[str] = None) -> List[int]:
        """
        Get active jobs.

        :param job_name_prefix: job name prefix e.g. stream_ingest_
        :return: List of active job ids
        """
        runs = self.jobs.list_runs(active_only=True)

        if "runs" in runs:
            if job_name_prefix:
                job_ids = [
                    run["job_id"]
                    for run in runs["runs"]
                    if run["run_name"].startswith(job_name_prefix)
                ]
            else:
                job_ids = [run["job_id"] for run in runs["runs"]]
        else:
            job_ids = []

        return job_ids

    def run_jobs(self, job_name_prefix: Optional[str] = None) -> List[int]:
        """
        Run jobs.

        :param job_name_prefix: job name prefix e.g. stream_ingest_
        :return: List of inactive job ids
        """
        job_ids = self.get_jobs(job_name_prefix)
        active_jobs_ids = self.get_active_jobs(job_name_prefix)

        inactive_jobs_ids = [
            job_id for job_id in job_ids if job_id not in active_jobs_ids
        ]

        for job_id in inactive_jobs_ids:
            self.jobs.run_now(job_id)

        return inactive_jobs_ids
