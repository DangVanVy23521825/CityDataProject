from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType

vehicleSchema = StructType([
    StructField("id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("location", StringType(), True),
    StructField("speed_kmh", DoubleType(), True),
    StructField("direction", StringType(), True),
    StructField("bearing_deg", DoubleType(), True),
    StructField("make", StringType(), True),
    StructField("model", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("fuelType", StringType(), True),
])

gpsSchema = StructType([
    StructField("id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("speed_kmh", DoubleType(), True),
    StructField("direction", StringType(), True),
    StructField("vehicleType", StringType(), True),
])

trafficSchema = StructType([
    StructField("id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("camera_id", StringType(), True),
    StructField("location", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
    ]), True),
    StructField("timestamp", TimestampType(), True),
    StructField("snapshot", StringType(), True),
])

weatherSchema = StructType([
    StructField("id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("location", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
    ]), True),
    StructField("timestamp", TimestampType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("weatherCondition", StringType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("windSpeed", DoubleType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("airQualityIndex", DoubleType(), True),
])