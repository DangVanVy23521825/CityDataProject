import logging
from pyspark.sql import SparkSession
from config import configuration
from schemas import vehicleSchema, gpsSchema, trafficSchema, weatherSchema
from utils.streaming import read_kafka_topic, streamWriter

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def main():
    spark = SparkSession.builder.appName("CityDataStreaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-southeast-2.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.access.key", configuration["AWS_ACCESS_KEY"]) \
        .config("spark.hadoop.fs.s3a.secret.key", configuration["AWS_SECRET_KEY"]) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # đọc dữ liệu từ Kafka -> streaming (partition topics)
    vehicleDF = read_kafka_topic(spark, "vehicle_data", vehicleSchema, configuration["KAFKA_BOOTSTRAP_SERVERS"])
    gpsDF = read_kafka_topic(spark, "gps_data", gpsSchema, configuration["KAFKA_BOOTSTRAP_SERVERS"])
    trafficDF = read_kafka_topic(spark, "traffic_data", trafficSchema, configuration["KAFKA_BOOTSTRAP_SERVERS"])
    weatherDF = read_kafka_topic(spark, "weather_data", weatherSchema, configuration["KAFKA_BOOTSTRAP_SERVERS"])

    # ghi ra S3
    s3 = configuration["S3_BUCKET"]
    query1 = streamWriter(vehicleDF, f"{s3}/checkpoints/vehicle_data", f"{s3}/data/vehicle_data")
    query2 = streamWriter(gpsDF, f"{s3}/checkpoints/gps_data", f"{s3}/data/gps_data")
    query3 = streamWriter(trafficDF, f"{s3}/checkpoints/traffic_data", f"{s3}/data/traffic_data")
    query4 = streamWriter(weatherDF, f"{s3}/checkpoints/weather_data", f"{s3}/data/weather_data")

    logger.info("Streaming started. Waiting for termination...")
    query4.awaitTermination()

if __name__ == "__main__":
    main()