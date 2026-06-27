from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import time
from datetime import datetime
import os

spark=(
    SparkSession.builder.appName("rapido_data") \
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

df_drivers=spark.readStream.format("kafka")\
            .option("kafka.bootstrap.servers","kafka:9092")\
            .option("subscribe","rapido.public.drivers")\
            .option("startingOffsets","earliest")\
            .load()

df_riders=spark.readStream.format("kafka")\
            .option("kafka.bootstrap.servers","kafka:9092")\
            .option("subscribe","rapido.public.riders")\
            .option("startingOffsets","earliest")\
            .load()
            
df_rides=spark.readStream.format("kafka")\
            .option("kafka.bootstrap.servers","kafka:9092")\
            .option("subscribe","rapido.public.rides")\
            .option("startingOffsets","earliest")\
            .load()
            
drivers_df=df_drivers.select(col("value").cast("string"))
riders_df=df_riders.select(col("value").cast("string"))
rides_df=df_rides.select(col("value").cast("string"))

driver_schema=StructType([
    StructField("op",StringType(),True),
    StructField("after",StructType([
        StructField("driver_id", StringType(), True),
        StructField("driver_name", StringType(), True),
        StructField("vehicle_type", StringType(), True),
        StructField("vehicle_no", StringType(), True),
        StructField("total_rides", IntegerType(), True),
        StructField("avg_rating", StringType(), True),  
        StructField("is_active", BooleanType(), True),
        StructField("created_at", LongType(), True)
    ]),True)
])

rider_schema=StructType([
    StructField("op",StringType(),True),
    StructField("after",StructType([
        StructField("rider_id",StringType(),True),
        StructField("rider_name",StringType(),True),
        StructField("phone",StringType(),True),
        StructField("email",StringType(),True),
        StructField("city",StringType(),True),
        StructField("total_rides",IntegerType(),True),
        StructField("avg_rating",StringType(),True),
        StructField("created_at",LongType(),True)
    ]),True)
])

ride_schema=StructType([
    StructField("op",StringType(),True),
    StructField("after",StructType([
        StructField("ride_id",StringType(),True),
        StructField("rider_id",StringType(),True),
        StructField("driver_id",StringType(),True),
        StructField("status",StringType(),True),
        StructField("pickup",StringType(),True),
        StructField("drop_loc",StringType(),True),
        StructField("vehicle_type",StringType(),True),
        StructField("fare",StringType(),True),
        StructField("distance_km",StringType(),True),
        StructField("rider_rating",StringType(),True),
        StructField("driver_rating",StringType(),True),
        StructField("created_at",LongType(),True),
        StructField("updated_at",LongType(),True)
    ]))
])

driver_data=drivers_df.select(from_json(col("value"),driver_schema).alias("data"))\
                    .select(col("data.op"),col("data.after.*"))
                    
rider_data=riders_df.select(from_json(col("value"),rider_schema).alias("data"))\
                    .select(col("data.op"),col("data.after.*"))
                    
ride_data=rides_df.select(from_json(col("value"),ride_schema).alias("data"))\
                    .select(col("data.op"),col("data.after.*"))
                    

driver_data.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation","s3a://rapido-data/bronze/checkpoint/drivers")\
            .option("path","s3a://rapido-data/bronze/drivers/")\
            .trigger(processingTime="1 seconds")\
            .start()
            
rider_data.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation","s3a://rapido-data/bronze/checkpoint/riders")\
            .option("path","s3a://rapido-data/bronze/riders/")\
            .trigger(processingTime="1 seconds")\
            .start()
            
ride_data.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation","s3a://rapido-data/bronze/checkpoint/rides")\
            .option("path","s3a://rapido-data/bronze/rides/")\
            .trigger(processingTime="1 seconds")\
            .start()
            
spark.streams.awaitAnyTermination()