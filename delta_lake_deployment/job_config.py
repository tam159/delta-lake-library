"""Databricks jobs pipelines configuration."""

CLUSTER_ID = "1111-999999-aaaaaaaa"
FAILURE_ALERT_EMAILS = ["npt.dc@outlook.com"]

EVENT_TOPICS = ("Transaction",)
CDC_TOPICS = ("Client",)

EVENT_TABLES = ("transaction",)

STREAM_JOB_PATH = "dbfs:/spark_jobs/stream"
STREAM_INGEST_KAFKA_PYSPARK = f"{STREAM_JOB_PATH}/ingest_kafka.py"
STREAM_EXTRACT_RAW_EVENT_PYSPARK = f"{STREAM_JOB_PATH}/extract_raw_event.py"

BATCH_JOB_PATH = "dbfs:/spark_jobs/batch"
BATCH_VACUUM_DATA_PYSPARK = f"{BATCH_JOB_PATH}/vacuum_data.py"
BATCH_VACUUM_DATA_CRON = "0 0 12 ? * Sun"
