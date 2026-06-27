from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
import os
spark=(
    SparkSession.builder.appName("rapido_silver_delta")\
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("spark://spark-master:7077") \
    .config("spark.streaming.StopGracefullyOnShutdown", "true") \
    ##aws
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID","")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY","")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()
)
df_driver=spark.read.format("delta").load("s3a://rapido-data/silver/drivers/")
df_rider=spark.read.format("delta").load("s3a://rapido-data/silver/riders/")
df_ride=spark.read.format("delta").load("s3a://rapido-data/silver/rides/")

print(df_driver.count(),df_rider.count(),df_ride.count())