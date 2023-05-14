from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from lib.logger import Log4j
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Spark Sink Demo") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .getOrCreate()

    logger = Log4j(spark)

    schema = StructType([
        StructField("InvoiceNumber", StringType()),
        StructField("CreatedTime", LongType()),
        StructField("StoreID", StringType()),
        StructField("PosID", StringType()),
        StructField("CashierID", StringType()),
        StructField("CustomerType", StringType()),
        StructField("CustomerCardNo", StringType()),
        StructField("TotalAmount", DoubleType()),
        StructField("NumberOfItems", IntegerType()),
        StructField("PaymentMethod", StringType()),
        StructField("CGST", DoubleType()),
        StructField("SGST", DoubleType()),
        StructField("CESS", DoubleType()),
        StructField("DeliveryType", StringType()),
        StructField("DeliveryAddress", StructType([
            StructField("AddressLine", StringType()),
            StructField("City", StringType()),
            StructField("State", StringType()),
            StructField("PinCode", StringType()),
            StructField("ContactNumber", StringType())
        ])),
        StructField("InvoiceLineItems", ArrayType(StructType([
            StructField("ItemCode", StringType()),
            StructField("ItemDescription", StringType()),
            StructField("ItemPrice", DoubleType()),
            StructField("ItemQty", IntegerType()),
            StructField("TotalValue", DoubleType())
        ]))),
    ])

    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "invoices") \
        .option("startingOffsets", "earliest") \
        .load()

    value_df = kafka_df.select(f.from_json(f.col("value").cast("string"), schema).alias("value"))
    notification_df = value_df.select("value.InvoiceNumber", "value.CustomerCardNo", "value.TotalAmount") \
        .withColumn("EarnedLoyaltyPoints", f.expr("TotalAmount*0.2"))
    #
    # notification_df.withColumn("value",
    #                            f.to_json(f.struct("CustomerCardNo", "TotalAmount", "EarnedLoyaltyPoints"))).show(
    #     truncate=False)
    # kafka_target_df = notification_df.selectExpr("InvoiceNumber as key",
    #                                              """to_json(
    #
    # """)
    kafka_target_df = notification_df.withColumnRenamed("InvoiceNumber", "key") \
        .withColumn("value",
                    f.to_json(f.struct("CustomerCardNo", "TotalAmount", "EarnedLoyaltyPoints"))) \
        .select("key", "value");

    notification_writer_query = kafka_target_df \
        .writeStream \
        .queryName("Notification Writer") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "notifications") \
        .option("checkpointLocation", "chk-point-dir") \
        .outputMode("append") \
        .start()

    logger.info("Listening and writing to kafka")
    notification_writer_query.awaitTermination();
