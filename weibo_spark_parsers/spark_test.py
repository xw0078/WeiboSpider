from pyspark import SparkFiles
from pyspark.sql import SparkSession
import logging
import os
from lxml import etree
import datetime
from datetime import datetime as dt
import re
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import Row


logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
spark_builder = (
    SparkSession
    .builder
    .master("local")
    .config('spark.mongodb.input.uri', 'mongodb://127.0.0.1:27017/Test')
    .config('spark.mongodb.output.uri', 'mongodb://127.0.0.1:27017/Test')
    .appName("Weibo Pyspark MongoDB Parser"))

spark = spark_builder.getOrCreate() # get spark session
logging.info("Spark Session Created")

df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1:27017/SWtest.test1").load()
df.show()
# rdd = df.rdd

# logging.info(transformed.take(2))
# transformed_df = transformed.toDF()
# transformed_df.write.format("mongo").mode("append").option("database","SWtest").option("collection", "test1").save()
