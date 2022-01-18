"""Spark jobs creation."""

from databricks_jobs import DatabricksJobs
from job_config import (
    BATCH_VACUUM_DATA_CRON,
    BATCH_VACUUM_DATA_PYSPARK,
    CDC_TOPICS,
    EVENT_TABLES,
    EVENT_TOPICS,
)

if __name__ == "__main__":
    databricks_jobs = DatabricksJobs()

    kafka_topics = {"event": EVENT_TOPICS, "cdc": CDC_TOPICS}
    for data_type, topics in kafka_topics.items():
        databricks_jobs.creat_stream_ingest_jobs(data_type, topics)

    databricks_jobs.creat_stream_extract_raw_event_jobs(EVENT_TABLES)

    databricks_jobs.creat_batch_job(
        job_name="batch_vacuum_data",
        python_file=BATCH_VACUUM_DATA_PYSPARK,
        quartz_cron_expression=BATCH_VACUUM_DATA_CRON,
    )

    print(databricks_jobs.jobs.list_jobs())
