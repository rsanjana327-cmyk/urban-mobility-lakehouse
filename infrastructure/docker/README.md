\# 🚕 Urban Mobility Data Lakehouse



A production-grade data lakehouse built on open-source technologies, processing \*\*8.7 million NYC Yellow Taxi trips\*\* through a fully automated 4-layer pipeline (Raw → Bronze → Silver → Gold) with ACID transactions, data quality enforcement, and business analytics.



\---



\## 🏗️ Architecture

```

NYC Taxi Data (TLC)

&#x20;       ↓

&#x20;  Raw Layer (MinIO)

&#x20;  Parquet files, Hive partitioning, idempotent ingestion

&#x20;       ↓

&#x20;  Bronze Layer (Delta Lake)

&#x20;  ACID transactions, audit columns, schema harmonization, CDF

&#x20;       ↓

&#x20;  Silver Layer (Delta Lake)

&#x20;  Deduplication, quality rules, derived metrics

&#x20;       ↓

&#x20;  Gold Layer (Delta Lake)

&#x20;  Business aggregations, pre-computed metrics

&#x20;       ↓

&#x20;  Orchestration (Apache Airflow)

&#x20;  4 DAGs, scheduled pipeline, ExternalTaskSensors

```



\---



\## 🛠️ Tech Stack



| Component | Technology |

|---|---|

| Processing | Apache Spark 3.5, PySpark |

| Storage Format | Delta Lake 3.0 |

| Object Storage | MinIO (S3-compatible) |

| Orchestration | Apache Airflow 2.8 |

| Streaming | Apache Kafka |

| Query Engine | Trino |

| Containerization | Docker, Docker Compose |

| Language | Python 3.11 |



\---



\## 📊 Pipeline Results



| Layer | Records | Description |

|---|---|---|

| Raw | 144 MB | 3 months NYC taxi Parquet files |

| Bronze | 9,384,487 | Delta Lake with audit columns |

| Silver | 8,746,676 | Deduplicated, quality-filtered |

| Gold | 3 tables | Business aggregations |



\### Key Metrics from Gold Layer

\- \*\*Busiest hour:\*\* 6pm with 627,473 trips

\- \*\*Top location:\*\* JFK Airport (Location 132) — $34.6M revenue

\- \*\*Peak revenue hours:\*\* 4pm-7pm evening rush



\---



\## 🔑 Key Engineering Concepts Implemented



\### 1. Idempotent Ingestion

Raw ingestion uses MD5 checksums to track uploaded files. Re-running the pipeline never creates duplicates in MinIO.



\### 2. Schema Harmonization

NYC taxi files have inconsistent schemas across months (INT vs BIGINT, column casing differences). A canonical schema with automated type casting resolves this before Bronze ingestion.



\### 3. ACID Transactions with Delta Lake

Bronze and Silver layers use Delta Lake format, providing:

\- Atomic writes (all-or-nothing)

\- Time travel (`VERSION AS OF`)

\- Change Data Feed for incremental processing

\- Schema evolution with `mergeSchema=true`



\### 4. Window Function Deduplication

Silver layer uses PySpark window functions to deduplicate 28.2M duplicate records, keeping the most recently ingested version of each trip.



\### 5. Airflow Orchestration

4 DAGs with ExternalTaskSensors ensure correct execution order. Each DAG retries automatically on failure.



\---



\## 📁 Project Structure

```

urban-mobility-lakehouse/

├── ingestion/

│   └── batch/upload\_to\_raw.py       # Idempotent MinIO uploader

├── transformation/

│   ├── bronze/batch\_bronze.py       # Raw → Bronze Delta Lake

│   ├── silver/batch\_silver.py       # Bronze → Silver (clean)

│   └── gold/batch\_gold.py           # Silver → Gold (aggregated)

├── orchestration/

│   └── dags/

│       ├── raw\_ingestion\_daily.py

│       ├── bronze\_processing\_daily.py

│       ├── silver\_transform\_daily.py

│       └── gold\_aggregation\_daily.py

├── infrastructure/

│   └── docker/

│       ├── docker-compose.yml          # Spark, MinIO, Kafka, Trino

│       └── docker-compose-airflow.yml  # Airflow stack

├── scripts/

│   ├── download\_data.py

│   └── download\_jars.py

└── data/

&#x20;   └── raw/                            # Local Parquet files

```



\---



\## 🚀 Quick Start



\### Prerequisites

\- Docker Desktop

\- Python 3.11

\- 8GB RAM minimum



\### 1. Clone the repo

```bash

git clone https://github.com/rsanjana327-cmyk/urban-mobility-lakehouse.git

cd urban-mobility-lakehouse

```



\### 2. Start infrastructure

```bash

cd infrastructure/docker

docker compose up -d

```



\### 3. Create virtual environment

```bash

python -m venv lakehouse\_env

lakehouse\_env\\Scripts\\Activate.ps1  # Windows

pip install pyspark==3.5.0 delta-spark==3.0.0 boto3 pandas pyarrow

```



\### 4. Download data and JARs

```bash

python scripts/download\_data.py

python scripts/download\_jars.py

```



\### 5. Run the pipeline

```bash

python ingestion/batch/upload\_to\_raw.py

python transformation/bronze/batch\_bronze.py

python transformation/silver/batch\_silver.py

python transformation/gold/batch\_gold.py

```



\### 6. Start Airflow

```bash

docker compose -f docker-compose-airflow.yml up -d

\# Open http://localhost:8085 (admin/admin)

```



\---



\## 📈 Data Quality Rules (Silver Layer)



| Rule | Description |

|---|---|

| `trip\_distance > 0` | No zero-distance trips |

| `fare\_amount > 0` | No zero or negative fares |

| `passenger\_count > 0` | At least 1 passenger |

| `pickup\_datetime NOT NULL` | Valid pickup time |

| `dropoff\_datetime NOT NULL` | Valid dropoff time |

| `dropoff > pickup` | Dropoff must be after pickup |

| `PULocationID NOT NULL` | Valid pickup location |

| `DOLocationID NOT NULL` | Valid dropoff location |



\---



\## 🔄 Airflow DAG Schedule



| DAG | Schedule | Description |

|---|---|---|

| raw\_ingestion\_daily | 00:00 daily | Download + upload to MinIO |

| bronze\_processing\_daily | 00:30 daily | Raw → Bronze Delta Lake |

| silver\_transform\_daily | 02:00 daily | Bronze → Silver clean |

| gold\_aggregation\_daily | 04:00 daily | Silver → Gold metrics |



\---



\## 💡 Interview Talking Points



\- \*\*Schema Evolution:\*\* Handled INT/BIGINT conflicts across monthly files using canonical schema harmonization

\- \*\*Exactly-Once Semantics:\*\* MD5 checksum idempotency prevents duplicate ingestion

\- \*\*ACID Guarantees:\*\* Delta Lake ensures no partial writes even on failure

\- \*\*Incremental Processing:\*\* Change Data Feed enables Silver to process only Bronze changes

\- \*\*Window Functions:\*\* Deduplication using `ROW\_NUMBER() OVER PARTITION BY` removed 28.2M duplicates

\- \*\*Query Performance:\*\* Gold layer pre-aggregations reduced scan from 8.7M to \~100 rows (100x improvement)



\---



\## 📝 License

MIT License — feel free to use this as a reference for your own projects.

