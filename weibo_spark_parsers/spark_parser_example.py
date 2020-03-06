from pyspark import SparkFiles
from pyspark.sql import SparkSession
import logging
import os
from lxml import etree


def main_html_parser(iterator):
    for element in iterator:
        tree_node = etree.HTML(element[3]) 
        tweet_nodes = tree_node.xpath('//div[@class="c" and @id]')
        for tweet_node in tweet_nodes:
            create_time_info_node = tweet_node.xpath('.//span[@class="ct"]')[-1]
            create_time_info = create_time_info_node.xpath('string(.)')
        yield [create_time_info[0].strip(),element[1]]


class weiboPySparkMDBParser:

    def __init__(self):
        logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
        spark_builder = (
            SparkSession
            .builder
            .master("local")
            .config('spark.mongodb.input.uri', 'mongodb://127.0.0.1:27017/Sina2.SearchPageRaw')
            .config('spark.mongodb.output.uri', 'mongodb://127.0.0.1:27017/Sina2.SearchPageRaw')
            .appName("Weibo Pyspark MongoDB Parser"))
        self.spark = spark_builder.getOrCreate() # get spark session
        logging.info("Spark Session Created")
    

    


    def findPotentialUser(self):
        df = self.spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1:27017/Sina2.SearchPageRaw").load()
        df.show(2)
        rdd = df.rdd
        transformed = rdd.mapPartitions(main_html_parser)
        logging.info(transformed.take(2))
        transformed_df = transformed.toDF(["created_time","crawl_time"])
        transformed_df.write.format("mongo").mode("append").option("database","SWtest").option("collection", "test1").save()

if __name__ == "__main__":
    parser = weiboPySparkMDBParser()
    parser.findPotentialUser()
    logging.info("END")

