"""Spark Kafka libraries."""

from typing import Dict, Tuple

from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr, from_json
from pyspark.sql.types import StringType

from delta_lake_library.schemas.cdc_mongodb import cdc_mongodb_schema


class SparkKafka:
    """
    Spark connect with Kafka.

    :param spark: SparkSession
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.dbutils = DBUtils(spark)

    @property
    def _get_schema_registry_config(self) -> Dict[str, str]:
        """
        Get Confluent schema registry configuration.

        :return: schema registry configuration
        """
        schema_registry_url = self.dbutils.secrets.get(
            scope="confluent_schema", key="schema_registry_url"
        )
        schema_registry_api_key = self.dbutils.secrets.get(
            scope="confluent_schema", key="schema_registry_api_key"
        )
        schema_registry_secret = self.dbutils.secrets.get(
            scope="confluent_schema", key="schema_registry_secret"
        )

        return {
            "url": schema_registry_url,
            "basic.auth.user.info": f"{schema_registry_api_key}:{schema_registry_secret}",
        }

    @property
    def _get_kafka_config(self) -> Tuple[str, str, str]:
        """
        Get Confluent kafka configuration.

        :return: kafka configuration
        """
        kafka_bootstrap_servers = self.dbutils.secrets.get(
            scope="confluent_broker", key="kafka_bootstrap_servers"
        )
        kafka_user = self.dbutils.secrets.get(
            scope="confluent_broker", key="kafka_user"
        )
        kafka_secret = self.dbutils.secrets.get(
            scope="confluent_broker", key="kafka_secret"
        )

        return kafka_bootstrap_servers, kafka_user, kafka_secret

    def get_schema(self, topic_name: str) -> str:
        """
        Get schema of a topic.

        :param topic_name: topic name
        :return: topic schema
        """
        schema_registry_client = SchemaRegistryClient(self._get_schema_registry_config)

        return schema_registry_client.get_latest_version(
            f"{topic_name}-value"
        ).schema.schema_str

    def read_stream(
        self, topic_name: str, starting_offsets: str = "earliest"
    ) -> DataFrame:
        """
        Read streaming Kafka.

        :param topic_name: topic name
        :param starting_offsets: starting offsets (e.g. latest, earliest, json string)
        :return: stream DataFrame
        """
        kafka_bootstrap_servers, kafka_user, kafka_secret = self._get_kafka_config

        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
            .option(
                "kafka.sasl.jaas.config",
                f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
                f"required username='{kafka_user}' password='{kafka_secret}';",
            )
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("subscribe", topic_name)
            .option("startingOffsets", starting_offsets)
            .option("groupIdPrefix", f"spark-kafka-source-{topic_name}")
            .load()
        )

    def read_batch(
        self,
        topic_name: str,
        starting_offsets: str = "earliest",
        ending_offsets: str = "latest",
    ) -> DataFrame:
        """
        Read batching Kafka.

        :param topic_name: topic name
        :param starting_offsets: starting offsets (e.g. earliest, json string)
        :param ending_offsets: ending offsets (e.g. latest, json string '{"topic_name":{"0":123}}')
        :return: stream DataFrame
        """
        kafka_bootstrap_servers, kafka_user, kafka_secret = self._get_kafka_config

        return (
            self.spark.read.format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
            .option(
                "kafka.sasl.jaas.config",
                f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
                f"required username='{kafka_user}' password='{kafka_secret}';",
            )
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("subscribe", topic_name)
            .option("startingOffsets", starting_offsets)
            .option("endingOffsets", ending_offsets)
            .option("groupIdPrefix", f"spark-kafka-source-{topic_name}")
            .load()
        )

    @staticmethod
    def get_structured_df(
        kafka_df: DataFrame,
        schema: str,
        deserialization_mode: str = "PERMISSIVE",
    ) -> DataFrame:
        """
        Get structured DataFrame from pure Kafka stream.

        :param kafka_df: kafka pure stream
        :param schema: kafka schema
        :param deserialization_mode: mode when deserialization fails (e.g. PERMISSIVE, FAILFAST)
        :return: structured DataFrame
        """
        value_alias = "extracted_value"
        avro_options = {"mode": deserialization_mode}

        return (
            kafka_df.select(
                "*",
                from_avro(
                    expr("substring(value, 6, length(value)-5)"), schema, avro_options
                ).alias(value_alias),
            )
            .select("*", f"{value_alias}.*")
            .drop("key", "value", value_alias)
        )

    @staticmethod
    def get_payload_df(
        kafka_df: DataFrame,
    ) -> DataFrame:
        """
        Get payload DataFrame from pure Kafka stream.

        :param kafka_df: kafka pure stream
        :return: payload DataFrame
        """
        value_alias = "extracted_value"

        return (
            kafka_df.withColumn("key", col("key").cast(StringType()))
            .withColumn("value", col("value").cast(StringType()))
            .select("*", from_json("value", cdc_mongodb_schema).alias(value_alias))
            .select("*", f"{value_alias}.schema", f"{value_alias}.payload.*")
            .drop(value_alias)
        )
