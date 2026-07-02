FROM apache/airflow:3.1.5

USER root
RUN apt-get update && apt-get install -y docker.io

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt