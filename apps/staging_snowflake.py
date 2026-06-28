from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

spark=(SparkSession.builder.appName("to_snowflake")\
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("spark://spark-master:7077") \
    .config("spark.streaming.StopGracefullyOnShutdown", "true") \
    ##aws
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID","")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY","")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate())

SNOWFLAKE_OPTION={
    "sfURL": os.environ.get("SNOWFLAKE_ACCOUNT")+".snowflakecomputing.com",
    "sfUser": os.environ.get("SNOWFLAKE_USER"),
    "sfPassword": os.environ.get("SNOWFLAKE_PASSWORD"),
    "sfDatabase": "RAPIDO",
    "sfSchema": "STAGING",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole":      "ACCOUNTADMIN",
}
tables=["drivers","riders","rides"]
for table in tables:
    df=spark.read.format("delta").load(f"s3a://rapido-data/silver/{table}/")
    print(f"read {table} table done")
    
    df.write.format("snowflake")\
            .options(**SNOWFLAKE_OPTION)\
            .option("dbtable",f"{table}")\
            .mode("overwrite")\
            .save()
    print(f"table {table} is done in staging")
    
spark.stop()
    