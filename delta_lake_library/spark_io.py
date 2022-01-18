"""Spark read write dataframe."""

from typing import Optional

from pyspark.sql import DataFrame, SparkSession


class SparkIO:
    """
    Spark read write DataFrame.

    :param spark: SparkSession
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_stream_cdf(
        self,
        table_name: str,
        starting_version: Optional[str] = None,
        starting_timestamp: Optional[str] = None,
    ) -> DataFrame:
        """
        Read stream change data feed from a table.

        :param table_name: table name
        :param starting_version: starting version, e.g. 0, "1" or "latest"
        :param starting_timestamp: starting timestamp, e.g. "2021-11-20 00:00:00"
        :return: change DataFrame
        """
        options = {"readChangeFeed": "true"}

        if starting_version:
            options["startingVersion"] = starting_version

        if starting_timestamp:
            options["startingTimestamp"] = starting_timestamp

        return (
            self.spark.readStream.format("delta").options(**options).table(table_name)
        )

    @staticmethod
    def write_stream(
        dataframe: DataFrame,
        table_name: str,
        checkpoint_location: str,
        format: str = "delta",
        output_mode: str = "append",
        merge_schema: str = "true",
        trigger_time: str = "15 seconds",
    ) -> None:
        """
        Write stream dataframe to a table.

        :param dataframe: DataFrame
        :param table_name: table name e.g. raw.merchant
        :param checkpoint_location: checkpoint location e.g. /mnt/raw/checkpoint/issuer
        :param format: write format e.g. delta, parquet, orc
        :param output_mode: output mode e.g. append, update, complete
        :param merge_schema: schema evolution e.g. true, false to raise error when schema changes
        :param trigger_time: trigger processing time
        :return: None
        """
        dataframe.writeStream.format(format).outputMode(output_mode).option(
            "mergeSchema", merge_schema
        ).option("checkpointLocation", checkpoint_location).trigger(
            processingTime=trigger_time
        ).toTable(
            table_name
        )
