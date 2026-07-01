from pyspark.sql import SparkSession
import os

spark = (
    SparkSession.builder.appName("check_cdf")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID",""))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY",""))
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .getOrCreate()
)

tables = ["drivers"]

for t in tables:
    path = f"s3a://rapido-data/bronze/{t}/"
    print(f"\n===== {t.upper()} =====")
    try:
        df = spark.sql(f"SHOW TBLPROPERTIES delta.`{path}`")
        df.show(truncate=False)
        
        # CDF specific check
        props = {row['key']: row['value'] for row in df.collect()}
        cdf_enabled = props.get('delta.enableChangeDataFeed', 'false')
        writer_version = props.get('delta.minWriterVersion', 'N/A')
        
        print(f"CDF Enabled: {cdf_enabled}")
        print(f"Min Writer Version: {writer_version}")
        
        if cdf_enabled == 'true':
            print(f"✅ {t}: CDF SAHI SE ENABLED HAI")
        else:
            print(f"❌ {t}: CDF ENABLE NAHI HUA")
    except Exception as e:
        print(f"⚠️ {t}: Table read nahi ho payi — {str(e)[:200]}")

spark.stop()