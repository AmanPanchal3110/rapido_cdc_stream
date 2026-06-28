from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
import os
from pyspark.sql.window import Window
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

df_drivers=spark.read.format("delta").load("s3a://rapido-data/bronze/drivers/")
df_riders=spark.read.format("delta").load("s3a://rapido-data/bronze/riders/")
df_rides=spark.read.format("delta").load("s3a://rapido-data/bronze/rides/")

df_rides=df_rides.filter(col("status").isin(["completed","cancelled"]))
print(df_rides.count())
