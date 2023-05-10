import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("File Streaming Demo") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()
    logger = Log4j(spark)
    # read
    raw_df = spark.readStream \
        .format("json") \
        .option("path", "input") \
        .option("maxFilesPerTrigger", "1") \
        .option("cleanSource", "delete") \
        .load()

    # transform
    explode_df = raw_df.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
                                   "CustomerType", "PaymentMethod", "DeliveryType",
                                   "DeliveryAddress.City", "DeliveryAddress.State",
                                   "DeliveryAddress.PinCode", "explode(InvoiceLineItems) as LineItem")
    flattened_df = explode_df \
        .withColumn("ItemCode", f.expr("LineItem.ItemCode")) \
        .withColumn("ItemDescription", f.expr("LineItem.ItemDescription")) \
        .withColumn("ItemPrice", f.expr("LineItem.ItemPrice")) \
        .withColumn("ItemQty", f.expr("LineItem.ItemQty")) \
        .withColumn("TotalValue", f.expr("LineItem.TotalValue")) \
        .drop("LineItem")

    # sink
    invoice_writer_query = flattened_df.writeStream \
        .format("json") \
        .option("path", "output") \
        .option("checkpointLocation", "chk-point-dir") \
        .outputMode("append") \
        .queryName("Flattened Invoice Writer") \
        .trigger(processingTime="1 minute") \
        .start()

    logger.info("Flattened Invoice Writer started")
    invoice_writer_query.awaitTermination()
