from airflow.sdk import dag,task
from airflow.utils.task_group import TaskGroup
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "io.delta:delta-spark_2.12:3.2.0"
)

PACKAGES_SNOWFLAKE = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "io.delta:delta-spark_2.12:3.2.0,"
    "net.snowflake:spark-snowflake_2.12:2.15.0-spark_3.4,"
    "net.snowflake:snowflake-jdbc:3.14.4"
)
@dag(
    schedule=None,
    start_date=datetime(2026,6,25),
    catchup=False,
    tags=["rapido"]
)
def rapido():
    silver_task=BashOperator(
        task_id="silver_task",
        bash_command=f"""docker exec spark-worker-1 \
            /opt/spark/bin/spark-submit \
            --conf spark.jars.ivy=/tmp/.ivy \
            --packages {PACKAGES} \
            /opt/spark-apps/silver_delta.py"""
    )
    staging_snoflake=BashOperator(
        task_id="staging-snowflake",
        bash_command=f"""docker exec spark-worker-1 \
            /opt/spark/bin/spark-submit \
            --conf spark.jars.ivy=/tmp/.ivy \
            --packages {PACKAGES_SNOWFLAKE} \
            /opt/spark-apps/staging_snowflake.py"""
    )
    
    silver_task >> staging_snoflake
    
rapido()
    
        