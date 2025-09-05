import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def read_kafka_topic(spark, topic, schema, kafka_bootstrap):
    logger.info(f"Reading topic {topic}")
    return (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), schema).alias("data"))
            .select("data.*")
            .withWatermark("timestamp", "2 minutes")
    )

def streamWriter(input: DataFrame, checkpointFolder, output):
    logger.info(f"Writing stream to {output} with checkpoint {checkpointFolder}")
    return (input.writeStream
            .format("parquet")
            .option("checkpointLocation", checkpointFolder)
            .option("path", output)
            .outputMode("append")
            .start())