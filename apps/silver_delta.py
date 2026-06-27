from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
import os
from delta.tables import DeltaTable
from pyspark.sql.window import Window
import json
import boto3

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
s3=boto3.client(
    "s3",
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY")
)

def get_last_version(table_name):
    key=f"silver/checkpoint/{table_name}_version.json"
    try:
        obj=s3.get_object(Bucket="rapido-data",Key=key)
        data=json.loads(obj["Body"].read())
        version=data["last_version"]
        print(f"last version {table_name} is{version}")
        return version
    except:
        print(f"{table_name} full read hogi")
        return None
    
def get_current_version(bronze_path):
    table=DeltaTable.forPath(spark,bronze_path)
    version=table.history(1).select("version").collect()[0][0]
    print(f"current version : {version}")
    return version

def save_last_version(table_name,version):
    key=f"silver/checkpoint/{table_name}_version.json"
    data=json.dumps({"last_version":version,"updated_at":str(datetime.now())})
    s3.put_object(Bucket="rapido-data",Key=key,Body=data)
    print(f"last version {version} of {table_name} is saved")
    
def read_bronze_cdf(table_name):
    bronze_path=f"s3a://rapido-data/bronze/{table_name}/"
    last_version=get_last_version(table_name)
    current_version=get_current_version(bronze_path)
    
    if last_version is not None and last_version==current_version:
        print(f"no new data for {table_name}")
        return None,current_version
    if last_version is None:
        print(f"first read full {table_name}")
        df=spark.read.format("delta").load(bronze_path)
        return df,current_version
    
    print(f"{table_name} cdf read {last_version+1} to {current_version}")
    df=spark.read.format("delta")\
        .option("readChangeFeed","true")\
        .option("startingVersion",last_version+1)\
        .option("endingVersion",current_version)\
        .load(bronze_path)
    df=df.filter(col("_change_type").isin("insert", "update_postimage", "delete"))\
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    return df,current_version

ZORDER_COLS = {
    "drivers": "vehicle_type",
    "riders":  "city",
    "rides":   "status, created_at",
}
def optimize(table_name):
    path=f"s3a://rapido-data/silver/{table_name}/"
    zorder=ZORDER_COLS[table_name]
    if not DeltaTable.isDeltaTable(spark,path):
        print(f"{table_name} silver ma nhi h optimise skip")
        return
    print(f"{table_name} optimise+zorder start")
    spark.sql(f"""OPTIMIZE delta.`{path}`
              ZORDER BY ({zorder});""")
    print(f"{table_name} optimise done")
    
def upsert(df,path,merge_key):
    if df is None:
        print(f"{path} no data")
        return
    df=df.withColumn("row_num",row_number().over(
        Window.partitionBy(merge_key).orderBy(col("updated_at").desc())
    )).filter(col("row_num") ==1).drop("row_num")
    df_insert=df.filter(col("op")=="c").drop("op")
    df_update=df.filter(col("op")=="u").drop("op")
    
    silver_exist=DeltaTable.isDeltaTable(spark,path)
    if not silver_exist:
        df_first=(
            df_insert.union(df_update) if df_insert.count()>0 and df_update.count()>0
            else df_insert if df_insert.count()>0
            else df_update
        )
        df_first.write.format("delta")\
                .option("delta.enableChangeDataFeed","true")\
                .mode("overwrite")\
                .save(path)
        print(f"table created {path}")
        return 
    table=DeltaTable.forPath(spark,path)
    if df_insert.count()>0 or df_update.count()>0:
        df_upsert=df_insert.union(df_update) if df_insert.count()>0 else df_update
        table.alias("trg").merge(df_upsert.alias("src"),f"trg.{merge_key}=src.{merge_key}")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
        print(f"upsert done {path}")
tables=["drivers","riders","rides"]
versions={}
dfs={}
for table in tables:
    df,version= read_bronze_cdf(table)
    dfs[table]=df
    versions[table]=version
    
if dfs["drivers"] is not None:
    dfs["drivers"]=dfs["drivers"].withColumn("created_at",to_timestamp(from_unixtime(col("created_at")/1000000)))\
                    .withColumn("updated_at",    to_timestamp(from_unixtime(col("updated_at") / 1000000)))\
                    .withColumn("avg_rating",col("avg_rating").cast(DecimalType(3,1)))
                    
if dfs["riders"] is not None:
    dfs["riders"]=dfs["riders"].withColumn("created_at",to_timestamp(from_unixtime(col("created_at")/1000000)))\
                .withColumn("updated_at",    to_timestamp(from_unixtime(col("updated_at") / 1000000)))\
                .withColumn("avg_rating",col("avg_rating").cast(DecimalType(3,1)))
                
if dfs["rides"] is not None:
    dfs["rides"]=dfs["rides"].withColumn("created_at",to_timestamp(from_unixtime(col("created_at") / 1000000)))\
            .withColumn("updated_at",    to_timestamp(from_unixtime(col("updated_at") / 1000000)))\
            .withColumn("driver_rating", col("driver_rating").cast(DecimalType(3, 1)))\
            .withColumn("rider_rating",  col("rider_rating").cast(DecimalType(3, 1)))\
            .withColumn("distance_km",   col("distance_km").cast(DecimalType(10, 2)))\
            .withColumn("fare",          col("fare").cast(DecimalType(10, 2)))\
            .filter(col("status").isin("completed", "cancelled"))
            
upsert(dfs["drivers"],"s3a://rapido-data/silver/drivers/","driver_id")
upsert(dfs["riders"],"s3a://rapido-data/silver/riders/","rider_id")
upsert(dfs["rides"],"s3a://rapido-data/silver/rides/","ride_id")

for table in tables:
    if dfs[table] is not None:
        save_last_version(table, versions[table])

for table in tables:
    optimize(table)

print("Silver done!")
spark.stop()