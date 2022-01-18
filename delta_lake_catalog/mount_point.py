"""Databricks mount point configuration."""

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

dbutils = DBUtils(spark)

mount_bucket = {
    "raw": "delta-lake-raw",
    "refined": "delta-lake-refined",
    "curated": "delta-lake-curated",
}


def mount_fs(env: str) -> None:
    """
    Mount S3 buckets to Databricks file system.

    :param env: environment (e.g. dev)
    :return: None
    """
    for mount, bucket in mount_bucket.items():
        dbutils.fs.mount(f"s3a://{bucket}-{env}", f"/mnt/{mount}")
