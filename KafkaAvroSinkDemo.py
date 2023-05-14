from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import to_avro
from pyspark.sql.functions import from_json, col, expr, struct
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

    value_df = kafka_df.withColumn("value", from_json(col("value").cast("string"), schema))

    explode_df = value_df.selectExpr("value.InvoiceNumber", "value.CreatedTime", "value.StoreID",
                                     "value.PosID", "value.CustomerType", "value.PaymentMethod", "value.DeliveryType",
                                     "value.DeliveryAddress.City", "value.DeliveryAddress.State",
                                     "value.DeliveryAddress.PinCode", "explode(value.InvoiceLineItems) as LineItem")

    flattened_df = explode_df.selectExpr("LineItem.ItemCode as ItemCode",
                                         "LineItem.ItemDescription as ItemDescription",
                                         "LineItem.ItemPrice as ItemPrice",
                                         "LineItem.ItemQty as ItemQty",
                                         "LineItem.TotalValue as TotalValue"
                                         ).drop("LineItem")
    kafka_target_df = flattened_df.select(expr("InvoiceNumber as key"),
                                          to_avro(struct("*")).alias("value"))

    invoice_writer_query = kafka_target_df.writeStream \
        .format("kafka") \
        .option("checkpointLocation", "chk-point-dir") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "invoice-items") \
        .outputMode("append") \
        .start()

    logger.info("Start Writer query")
    invoice_writer_query.awaitTermination()
