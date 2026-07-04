from airflow.sdk import dag
from airflow.operators.bash import BashOperator
from datetime import datetime
from datetime import timedelta
import pendulum

IST = pendulum.timezone("Asia/Kolkata")

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
    start_date=datetime(2026, 6, 25, tzinfo=IST),
    catchup=False,
    tags=["rapido"],
    max_active_runs=1,
)
def rapido():
    silver_task=BashOperator(
        task_id="silver_task",
        bash_command=f"""docker exec spark-master \
            /opt/spark/bin/spark-submit \
            --conf spark.jars.ivy=/tmp/.ivy \
            --packages {PACKAGES} \
            /opt/spark-apps/silver_delta.py""",
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=20)
    )
    raw_snoflake=BashOperator(
        task_id="raw_snowflake",
        bash_command=f"""docker exec spark-master \
            /opt/spark/bin/spark-submit \
            --conf spark.jars.ivy=/tmp/.ivy \
            --packages {PACKAGES_SNOWFLAKE} \
            /opt/spark-apps/raw_snowflake.py""",
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=20)
    )
    dbt_snowflake=BashOperator(
        task_id="dbt_snowflake",
        bash_command="docker exec dbt_core dbt run",
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=5)
    )
    dbt_test=BashOperator(
        task_id="dbt_test",
        bash_command=f"docker exec dbt_core dbt test",
        execution_timeout=timedelta(minutes=5)
    )
    dbt_docs=BashOperator(
        task_id="dbt_docs",
        bash_command="""
            docker exec dbt_core dbt deps &&
            docker exec dbt_core dbt docs generate
            """,
        execution_timeout=timedelta(minutes=5)
    )
    
    silver_task >> raw_snoflake >> dbt_snowflake >> dbt_test >> dbt_docs
    
rapido()
    
        