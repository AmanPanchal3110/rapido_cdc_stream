from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
import os
from delta.tables import DeltaTable
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
df_driver=spark.read.format("delta").load("s3a://rapido-data/bronze/drivers/")
df_rider=spark.read.format("delta").load("s3a://rapido-data/bronze/riders/")
df_ride=spark.read.format("delta").load("s3a://rapido-data/bronze/rides/")

df_driver=df_driver.withColumn("created_at",to_timestamp(from_unixtime(col("created_at")/1000000)))\
                    .withColumn("avg_rating",col("avg_rating").cast(DecimalType(3,1)))
                    
df_rider=df_rider.withColumn("created_at",to_timestamp(from_unixtime(col("created_at")/1000000)))

df_ride=df_ride.withColumn("created_at",to_timestamp(from_unixtime(col("created_at")/1000000)))\
                .withColumn("updated_at",to_timestamp(from_unixtime(col("updated_at")/1000000)))\
                .withColumn("driver_rating",col("driver_rating").cast(DecimalType(3,1)))\
                .withColumn("rider_rating",col("rider_rating").cast(DecimalType(3,1)))\
                .withColumn("distance_km",col("distance_km").cast(DecimalType(10,2)))\
                .withColumn("fare",col("fare").cast(DecimalType(10,2)))
df_ride=df_ride.filter(col("status").isin("completed", "cancelled"))

def upsert(df,path,merge_key):
    if "updated_at" in df.columns:
        df=df.withColumn("row_num",row_number().over(
            Window.partitionBy(merge_key).orderBy(col("updated_at").desc())
        )).filter(col("row_num")==1).drop("row_num")
    elif "created_at" in df.columns:
        df=df.withColumn("row_num",row_number().over(
            Window.partitionBy(merge_key).orderBy(col("created_at").desc())
        )).filter(col("row_num")==1).drop("row_num")
        
    df_insert=df.filter(col("op")=="c").drop("op")
    df_update=df.filter(col("op")=="u").drop("op")
    
    silver_exists=DeltaTable.isDeltaTable(spark,path)
    if not silver_exists:
        df_first = (
            df_insert.union(df_update)
            if df_insert.count() > 0 and df_update.count() > 0
            else df_insert if df_insert.count() > 0
            else df_update
        )
        df_first.write.format("delta").mode("overwrite").save(path)
        print("silver_table created")
        return
    
    table=DeltaTable.forPath(spark,path)
    if df_insert.count()>0 or df_update.count()>0:
        df_upsert=df_insert.union(df_update) if df_insert.count()>0 else df_update
        
        table.alias("trg").merge(df_upsert.alias("src"),f"trg.{merge_key}=src.{merge_key}")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
            
upsert(df_driver,"s3a://rapido-data/silver/drivers/","driver_id")
upsert(df_rider,"s3a://rapido-data/silver/riders/","rider_id")
upsert(df_ride,"s3a://rapido-data/silver/rides/","ride_id")

print("done")
spark.stop()
        
