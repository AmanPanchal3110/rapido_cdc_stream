# 🚀 Rapido CDC Real-Time Data Pipeline

> A production-grade real-time data pipeline built using Change Data Capture (CDC) to stream ride-sharing data from PostgreSQL to Snowflake with automated transformations using Apache Spark, Delta Lake, dbt, and Apache Airflow.

---

## 📌 Project Overview

This project simulates a real-world ride-sharing platform (like Rapido/Uber) data pipeline. It captures every database change in real-time using CDC (Change Data Capture), processes it through a medallion architecture on AWS S3, loads it into Snowflake, and transforms it using dbt for analytics.

**What makes this project special:**
- Real-time CDC capturing every INSERT, UPDATE, DELETE from PostgreSQL
- Medallion architecture (Bronze → Silver → Snowflake → Gold)
- Multi-threaded ride simulation with race condition handling
- Incremental Silver processing using Delta Lake CDF (Change Data Feed)
- Complete data quality testing with dbt

---

## 🏗️ Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                     RAPIDO DATA PIPELINE                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────┐    ┌────────────┐    ┌──────────────────────┐   │
│  │ Python Faker │───▶│ PostgreSQL │───▶│ Debezium CDC         │   │
│  │ (Fake Rides) │    │ (Source)   │    │ (WAL Log Reader)     │   │
│  └──────────────┘    └────────────┘    └──────────┬───────────┘   │
│                                                   │               │
│                                          ┌────────▼───────┐       │
│                                          │ Apache Kafka   │       │
│                                          │ (6 Partitions) │       │
│                                          └────────┬───────┘       │
│                                                   │               │
│                                    ┌──────────────▼─────────────┐ │
│                                    │ Spark Structured Streaming  │ │
│                                    │ (Bronze Layer — 24/7)       │ │
│                                    └──────────────┬─────────────┘ │
│                                                   │               │
│                                          ┌────────▼───────┐       │
│                                          │  AWS S3        │       │
│                                          │  Bronze Layer  │       │
│                                          │  (Delta Lake)  │       │
│                                          └────────┬───────┘       │
│                                                   │               │
│              ┌────────────────────────────────────▼─────────────┐ │
│              │           Apache Airflow (Every 30 min)           │ │
│              │                                                   │ │
│              │  ┌──────────┐  ┌──────────┐  ┌───────────────┐  │ │
│              │  │  Silver  │  │Snowflake │  │  dbt run      │  │ │
│              │  │  Delta   │─▶│ STAGING  │─▶│  dbt test     │  │ │
│              │  │  (CDF)   │  │ (Spark)  │  │  dbt docs     │  │ │
│              │  └──────────┘  └──────────┘  └───────┬───────┘  │ │
│              └──────────────────────────────────────┼───────────┘ │
│                                                     │             │
│                                          ┌──────────▼──────────┐  │
│                                          │   Snowflake MARTS   │  │
│                                          │  dim_drivers        │  │
│                                          │  dim_riders         │  │
│                                          │  fact_rides         │  │
│                                          │  agg_daily          │  │
│                                          │  obt_rides          │  │
│                                          └─────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🔄 Complete Pipeline Flow

```
Step 1: Python Faker → PostgreSQL (Seed 50 drivers, 200 riders)
Step 2: Python Threads → PostgreSQL (20 concurrent rides simulation)
Step 3: PostgreSQL WAL → Debezium → Kafka (CDC real-time)
Step 4: Kafka → Spark Streaming → S3 Bronze (Always running)
Step 5: Airflow (every 30 min):
        ├── S3 Bronze → S3 Silver (CDF incremental + MERGE + OPTIMIZE)
        ├── S3 Silver → Snowflake STAGING (Spark overwrite)
        └── dbt run → dbt test → dbt docs generate
```

---

## 🛠️ Tech Stack

| Category | Tool | Version | Purpose |
|----------|------|---------|---------|
| Source Database | PostgreSQL | 16 | Source of truth |
| CDC Tool | Debezium | 3.0.0 | WAL log capture |
| Message Broker | Apache Kafka | KRaft | Event streaming |
| Stream Processing | Apache Spark | 3.5.1 | Bronze + Silver |
| Data Lake Storage | AWS S3 + Delta Lake | 3.2.0 | Medallion layers |
| Orchestration | Apache Airflow | 3.x | Pipeline scheduling |
| Data Warehouse | Snowflake | — | Analytics store |
| Transformation | dbt-core + dbt-snowflake | 1.8.0 | Data modeling |
| Containerization | Docker + Compose | — | Infrastructure |
| Language | Python | 3.11 | All scripts |

---

## 📊 Data Simulation

### Fake Data Generation

The pipeline uses Python `Faker` library with `psycopg2.pool` and `threading` to simulate real-world ride data.

**Seed Data (runs once):**
- 50 drivers with vehicle info (bike/car/auto)
- 200 riders with city and contact info
- All start with `total_rides=0`, `avg_rating=0.0`

**Ride Simulation (runs continuously):**
- 20 concurrent threads using `threading.Lock()` for race condition prevention
- Each ride goes through: `requested → confirmed → driver_reached → picked_up → on_destination → completed`
- 20% rides randomly cancelled (DELETE event for CDC)
- Driver and rider `avg_rating` + `total_rides` updated on completion

**CDC Events Generated:**

| Event | CDC Operation | When |
|-------|--------------|------|
| New ride booked | INSERT (op=c) | status=requested |
| Status changed | UPDATE (op=u) | Every status transition |
| Rating added | UPDATE (op=u) | Ride completed |
| Ride cancelled | DELETE (op=d) | 20% of rides |
| Driver updated | UPDATE (op=u) | After completion |

---

## 🗂️ Medallion Architecture

### Bronze Layer (S3 — Always Streaming)
- Raw CDC events from Kafka
- Append-only Delta Lake tables
- All operations captured: INSERT, UPDATE, DELETE
- Debezium `op` field preserved (c/u/d)
- Timestamps in microseconds (Debezium format)
- CDF enabled for incremental Silver processing

```
s3://rapido-data/bronze/
├── drivers/    ← _delta_log + parquet files
├── riders/     ← _delta_log + parquet files
└── rides/      ← _delta_log + parquet files
```

### Silver Layer (S3 — Airflow Batch)
- Incremental processing using Delta Lake CDF
- Version tracking in S3 checkpoints
- Window function for latest state per ID
- MERGE (upsert) operation
- Timestamps converted to proper format
- `is_busy`, `is_riding` operational columns dropped
- Rides filtered: only `completed` + `cancelled`
- OPTIMIZE + ZORDER for query performance

```
s3://rapido-data/silver/
├── drivers/    ← latest state, cleaned
├── riders/     ← latest state, cleaned
└── rides/      ← completed + cancelled only

s3://rapido-data/silver/checkpoints/
├── drivers_version.json
├── riders_version.json
└── rides_version.json
```

### Snowflake STAGING (Airflow Batch)
- Spark reads Silver Delta Lake
- Overwrites Snowflake STAGING tables
- No duplicates (Silver already clean)
- 50 drivers, 200 riders always consistent
- Rides grow as simulation runs

### Snowflake MARTS (dbt)
- dbt staging views on STAGING tables
- dbt mart tables with business logic
- dbt tests for data quality
- dbt docs for documentation

---

## 📐 dbt Data Models

### Staging Layer (Views)

**stg_drivers:**
- Joins rides to calculate completed/cancelled per driver
- Adds `cancellation_rate`, `driver_activity`, `rating_category`

**stg_riders:**
- Joins rides to calculate completed/cancelled per rider
- Adds `total_spend`, `cancellation_rate`, `rider_activity`

**stg_rides:**
- Adds date parts: `ride_date`, `ride_hour`, `ride_month`, `ride_year`

### Marts Layer (Tables)

| Model | Description | Key Columns |
|-------|-------------|-------------|
| `dim_drivers` | Driver dimension | activity, rating_category, cancellation_rate |
| `dim_riders` | Rider dimension | activity, rating_category, total_spend |
| `fact_rides` | Ride transactions | fare, distance, ratings, date parts |
| `agg_daily` | Daily metrics | revenue, cancel_rate, avg_fare |
| `agg_driver_perf` | Driver KPIs | revenue per driver, avg fare |
| `obt_rides` | One Big Table | All joined — rides + drivers + riders |

### dbt Tests

| Test | Models | Description |
|------|--------|-------------|
| `unique` | All PKs | No duplicate IDs |
| `not_null` | All PKs, fare | No null values |
| `relationships` | fact_rides | FK integrity to dims |
| `accepted_values` | status, vehicle_type | Valid enum values |
| `accepted_range` | fare, rating | fare > 0, rating 0-5 |

---

## ⚙️ Key Technical Decisions

### Why Debezium over polling?
Debezium reads PostgreSQL WAL logs directly — capturing every change in real-time without any database load, unlike polling which adds query overhead.

### Why Delta Lake over plain Parquet?
Delta Lake provides ACID transactions, MERGE operations, time travel, and CDF (Change Data Feed) — critical for incremental processing and exactly-once semantics.

### Why CDF for Silver?
Instead of reading the entire Bronze table every 30 minutes, CDF tracks which versions were last processed and reads only new changes — reducing processing time significantly.

### Why threading.Lock() in ride simulation?
Multiple threads selecting available drivers simultaneously could cause race conditions where 2 threads pick the same driver. Lock ensures atomic `SELECT + UPDATE is_busy=TRUE` operations.

### Why OPTIMIZE + ZORDER?
Bronze streaming creates many small parquet files. OPTIMIZE compacts them into 128MB files. ZORDER BY (status, created_at) co-locates related data — making analytical queries 3x faster.

---

## 📈 Performance Metrics

| Metric | Value |
|--------|-------|
| Concurrent rides | 20 threads |
| CDC events/sec | ~60-70 |
| Kafka partitions | 6 |
| Spark workers | 2 × (2 cores, 2GB) |
| Pipeline schedule | Every 30 minutes |
| Silver processing | Incremental (CDF) |
| Bronze trigger | 1 second micro-batch |

---

## 🚀 How to Run

### Prerequisites
- Docker Desktop (8GB RAM minimum)
- AWS Account with S3 bucket named `rapido-data`
- Snowflake Account
- Python 3.11+

### Environment Variables (.env)
```bash
# AWS
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret

# Snowflake
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password

# Airflow
AIRFLOW_UID=50000
```

### Step by Step Setup

```bash
# 1. Clone repository
git clone https://github.com/AmanPanchal3110/rapido_cdc_stream.git
cd rapido_cdc_stream

# 2. Copy env file
cp .env.example .env
# Edit .env with your credentials

# 3. Start all Docker services
docker compose up -d

# 4. Wait for services to be healthy (2-3 minutes)
docker compose ps

# 5. Seed initial data (50 drivers, 200 riders)
python data/rapido_data.py

# 6. Register Debezium CDC connector
python data/kafka-debezium-connector.py

# 7. Start Bronze Spark Streaming (keep running)
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.cores.max=2 \
    /opt/spark-apps/bronze_delta.py

# 8. Start ride simulation (keep running)
python data/rides_data.py

# 9. Trigger Airflow DAG
# Open http://localhost:8085
# Enable and trigger 'rapido' DAG

# 10. View dbt docs
docker exec -d dbt_core dbt docs serve --port 8088 --no-browser
# Open http://localhost:8088
```

### Snowflake Setup (one time)
```sql
CREATE DATABASE RAPIDO;
CREATE SCHEMA RAPIDO.STAGING;
CREATE SCHEMA RAPIDO.MARTS;

CREATE TABLE RAPIDO.STAGING.DRIVERS (...);
CREATE TABLE RAPIDO.STAGING.RIDERS (...);
CREATE TABLE RAPIDO.STAGING.RIDES (...);
```

---

## 🌐 Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8085 | airflow / airflow |
| Spark Master | http://localhost:9091 | — |
| Spark Worker 1 | http://localhost:8081 | — |
| Kafka UI | http://localhost:9090 | — |
| Debezium UI | http://localhost:8080 | — |
| dbt Docs | http://localhost:8088 | — |

---

## 📁 Project Structure

```
rapido_cdc_stream/
│
├── apps/                          # Spark scripts
│   ├── bronze_delta.py            # Kafka → S3 Bronze (streaming)
│   ├── silver_delta.py            # Bronze → Silver (CDF + MERGE)
│   └── raw_snowflake.py           # Silver → Snowflake STAGING
│
├── data/                          # Data generation
│   ├── rapido_data.py             # Seed drivers + riders
│   ├── rides_data.py              # Real-time ride simulation
│   └── kafka-debezium-connector.py # CDC connector setup
│
├── dags/                          # Airflow DAGs
│   └── rapido_dag.py              # Main pipeline DAG
│
├── dbt_project/                   # dbt project
│   ├── models/
│   │   ├── staging/               # Views on STAGING
│   │   │   ├── sources.yml
│   │   │   ├── stg_drivers.sql
│   │   │   ├── stg_riders.sql
│   │   │   └── stg_rides.sql
│   │   └── marts/                 # Final tables
│   │       ├── dim_drivers.sql
│   │       ├── dim_riders.sql
│   │       ├── fact_rides.sql
│   │       ├── agg_daily.sql
│   │       ├── agg_driver_perf.sql
│   │       ├── obt_rides.sql
│   │       └── schema.yml         # dbt tests
│   ├── macros/
│   │   └── generate_schema_name.sql
│   ├── packages.yml
│   └── dbt_project.yml
│
├── docker-compose.yml             # All services
├── Dockerfile.dbt                 # dbt container
├── .env.example                   # Environment template
├── .gitignore
└── README.md
```

---

## 📸 Screenshots

### 1. Fake Data Generation — Python Faker
[Screenshot: rapido_data.py running — 50 drivers + 200 riders inserted]

### 2. Ride Simulation — 20 Concurrent Threads
[Screenshot: rides_data.py running — INSERT/UPDATE/DELETE events]

### 3. Debezium CDC Connector
[Screenshot: Debezium UI showing connector RUNNING status]

### 4. Kafka UI — Topics with Messages
[Screenshot: Kafka UI showing rapido.public.rides topic with messages]

### 5. Bronze Streaming — Spark UI
[Screenshot: Spark UI showing streaming job running]

### 6. Airflow DAG — Successful Run
[Screenshot: Airflow DAG with all green tasks]

### 7. S3 Bronze + Silver — Delta Lake Files
[Screenshot: S3 bucket showing _delta_log + parquet files]

### 8. Snowflake STAGING Tables
[Screenshot: Snowflake showing drivers, riders, rides tables]

### 9. Snowflake MARTS — dbt Models
[Screenshot: Snowflake showing dim, fact, agg, obt tables]

### 10. dbt Docs — Lineage Graph
[Screenshot: dbt docs showing model lineage]

### 11. dbt Test Results — All Passed
[Screenshot: dbt test showing all PASS]

---

## 🎯 What I Learned

- **CDC Design** — How Debezium reads PostgreSQL WAL logs and captures every change
- **Delta Lake** — MERGE, CDF, OPTIMIZE, ZORDER for efficient data lake management
- **Spark Streaming** — Micro-batch processing, checkpoints, exactly-once semantics
- **Race Conditions** — threading.Lock() for safe concurrent database operations
- **Incremental Processing** — CDF version tracking to avoid full table scans
- **dbt Modeling** — Staging → Marts pattern, testing, documentation
- **Airflow Orchestration** — DAG design, retry logic, task dependencies
- **Data Quality** — dbt tests for referential integrity, value validation

---

## 🔮 Future Improvements

- [ ] Add Apache Flink for true real-time Silver processing
- [ ] Implement Great Expectations for data quality monitoring
- [ ] Add Grafana + Prometheus for pipeline monitoring
- [ ] Implement dbt snapshots for SCD Type 2
- [ ] Add data masking for PII fields (phone, email)
- [ ] Deploy on AWS EKS with Kubernetes

---

## 👨‍💻 Author

**Aman Panchal**
- GitHub: [@AmanPanchal3110](https://github.com/AmanPanchal3110)

---

## 📄 License

MIT License — feel free to use this project for learning!