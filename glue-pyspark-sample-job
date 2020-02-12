import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext())
spark = glueContext.spark_session

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("s3a://qiqi/sample.csv")

df.groupBy("YEARMONTH", "LOCATION") \
    .count() \
    .orderBy("count", ascending=False) \
    .repartition(1) \
    .write \
    .format("csv") \
    .option("header","true") \
    .mode("Overwrite") \
    .save("s3a://qiqi/result")
