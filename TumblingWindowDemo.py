from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import to_avro, from_avro
from pyspark.sql.functions import from_json, col, expr, struct, sum, to_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Kafka Avro Sink Demo") \
        .master("local[*]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    logger = Log4j(spark)

    avroSchema = open('schema/invoice-items', mode='r').read()

    kafka_source_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "invoice-items") \
        .option("startingOffsets", "earliest") \
        .load()

    value_df = kafka_source_df.select(from_avro(col("value"), avroSchema).alias("value"))

    rewards_df = value_df.filter("value.CustomerType == 'PRIME'") \
        .groupBy("value.CustomerCardNo") \
        .agg(sum("value.TotalValue").alias("TotalPurchase"),
             sum(expr("value.TotalValue * 0.2").cast("integer")).alias("AggregatedRewards"))

    kafka_target_df = rewards_df.select(expr("CustomerCardNo as key"),
                                        to_json(struct("TotalPurchase", "AggregatedRewards")).alias("value"))
    rewards_writer_query = kafka_target_df.writeStream \
        .format("kafka") \
        .queryName("Rewards Writer") \
        .option("checkpointLocation", "chk-point-dir") \
        .option("topic", "customer-rewards") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .outputMode("update") \
        .start()

    rewards_writer_query.awaitTermination()
