from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("test")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

df = spark.createDataFrame([(1, "A")], ["id", "name"])

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.enableChangeDataFeed", "true") \
    .save("s3a://rapido-data/test_cdf")

spark.sql("""
SHOW TBLPROPERTIES delta.`s3a://rapido-data/test_cdf`
""").show(truncate=False)