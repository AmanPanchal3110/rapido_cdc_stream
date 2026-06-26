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
df_driver=spark.read.format("delta").load("s3a://rapido-data/bronze/drivers/")
df_rider=spark.read.format("delta").load("s3a://rapido-data/bronze/riders/")
df_ride=spark.read.format("delta").load("s3a://rapido-data/bronze/rides/")

df_driver=df_driver.withColumn("created_at",to_date(from_unixtime(col("created_at")/1000000)))\
                    .withColumn("avg_rating",col("avg_rating").cast(DecimalType(3,1)))
                    
df_rider=df_rider.withColumn("created_at",to_date(from_unixtime(col("created_at")/1000000)))

df_ride=df_ride.withColumn("created_at",to_date(from_unixtime(col("created_at")/1000000)))\
                .withColumn("updated_at",to_date(from_unixtime(col("updated_at")/1000000)))\
                .withColumn("driver_rating",col("driver_rating").cast(DecimalType(3,1)))\
                .withColumn("rider_rating",col("rider_rating").cast(DecimalType(3,1)))\
                .withColumn("distance_km",col("distance_km").cast(DecimalType(10,2)))\
                .withColumn("fare",col("fare").cast(DecimalType(10,2)))
df_ride.show()
df_ride.printSchema()
df_driver.show()
df_rider.show()
