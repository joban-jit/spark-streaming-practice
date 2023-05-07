from pyspark.sql import SparkSession
from lib.logger import Log4j
import pyspark.sql.functions as f

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("HelloSpark").getOrCreate()
    logger = Log4j(spark)
    surveyDF = spark.read\
        .option("header", True) \
        .option("inferSchema", True) \
        .csv("data/sample.csv")

    countDF = surveyDF.filter(surveyDF['Age']<40).groupBy("Country").agg(f.count("*")).alias("count")
    countDF.show()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
