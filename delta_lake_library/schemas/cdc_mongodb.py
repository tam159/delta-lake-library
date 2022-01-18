"""CDC MongoDB schema."""

from pyspark.sql.types import LongType, StringType, StructField, StructType

cdc_mongodb_schema = StructType(
    [
        StructField("schema", StringType(), True),
        StructField(
            "payload",
            StructType(
                [
                    StructField("after", StringType(), True),
                    StructField("patch", StringType(), True),
                    StructField("filter", StringType(), True),
                    StructField("source", StringType(), True),
                    StructField("op", StringType(), True),
                    StructField("ts_ms", LongType(), True),
                    StructField("transaction", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)
