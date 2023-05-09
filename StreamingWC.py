from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("StreamingWC") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true")\
        .config("spark.sql.shuffle.partitions", 3)\
        .getOrCreate()
    logger = Log4j(spark)
    # Create DataFrame representing the stream of input lines from connection to localhost:9999
    lines_df = spark.readStream.format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()
    lines_df.printSchema()
    # Split the lines into words
    words_df = lines_df.withColumn('word', f.explode(f.split(lines_df.value, ' ')))
    count_df = words_df.groupBy("word").count()

    # Start running the query that prints the running counts to the console
    query = count_df.writeStream \
        .format("console") \
        .option("checkpointLocation", "chk-point-dir") \
        .outputMode("complete") \
        .start()
    query.awaitTermination()
