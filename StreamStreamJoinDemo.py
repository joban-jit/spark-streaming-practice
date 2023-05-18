from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import from_json, col, expr, struct, sum, to_json, first, to_timestamp, window, max
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Stream Stream Join Demo") \
        .master("local[*]") \
        .config("spark.sql.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 2) \
        .getOrCreate()

    logger = Log4j(spark)

    impressionSchema = StructType([
        StructField("InventoryID", StringType()),
        StructField("CreatedTime", StringType()),
        StructField("Campaigner", StringType())
    ])

    clickSchema = StructType([
        StructField("InventoryID", StringType()),
        StructField("CreatedTime", StringType())
    ])

    kafka_impression_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "impressions") \
        .option("startingOffsets", "earliest") \
        .load()

    impressions_df = kafka_impression_df \
        .select(from_json(col("value").cast("string"), impressionSchema).alias("value")) \
        .select(expr("value.InventoryID as ImpressionID"), "value.CreatedTime", "value.Campaigner") \
        .withColumn("ImpressionTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss")) \
        .drop("CreatedTime")
    impressions_df.printSchema()

    kafka_click_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "clicks") \
        .option("startingOffsets", "earliest") \
        .load()

    clicks_df = kafka_click_df \
        .select(from_json(col("value").cast("string"), clickSchema).alias("value")) \
        .select(expr("value.InventoryID as ClickID"), "value.CreatedTime") \
        .withColumn("ClickTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss")) \
        .drop("CreatedTime")
    clicks_df.printSchema()

    joinExpr = "ImpressionID==ClickID"
    join_type = "inner"

    joined_df = impressions_df.join(clicks_df, expr(joinExpr), join_type)

    output_df = joined_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="1 minute") \
        .start()
    logger.info("Waiting for Query")
    output_df.awaitTermination()
