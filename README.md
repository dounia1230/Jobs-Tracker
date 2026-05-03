# Moroccan Job Market Analyzer

> **Academic Project — IADATA Program, EMSI Casablanca (2025/2026)**
> Big Data Platform for analyzing the Moroccan job market

---

## 🎯 What This Platform Does

Automatically collects job postings from Moroccan and international job boards every hour, processes them through a **Medallion Architecture** (Bronze → Silver → Gold), and surfaces insights through interactive dashboards:

- Top 20 most demanded technical skills in Morocco
- Which cities have the most job openings
- Salary trends for data engineers, developers, etc.
- Companies hiring the most right now
- Remote vs on-site job ratios
- CDI vs CDD vs Stage vs Freelance distribution

---

## 🏗️ Architecture

```
Job Sites → Scrapers → Kafka → MinIO (Bronze)
                                    ↓
                             silver_transform.py
                                    ↓
                             MinIO (Silver)
                                    ↓
                            gold_aggregations.py
                                    ↓
                    MinIO (Gold) + PostgreSQL (DW)
                                    ↓
                              Metabase Dashboards
```

**All orchestrated by Apache Airflow — runs every hour automatically.**

---

## 🚀 Quick Start (One Command)

### Prerequisites
- Docker Desktop installed and running
- At least 8 GB RAM available (16 GB recommended)
- At least 20 GB disk space

### Launch the full stack

```bash
# 1. Clone / navigate to project
cd moroccan-job-analyzer

# 2. Copy environment file
cp .env.example .env   # (already provided, no changes needed)

# 3. Initialize Airflow database (first time only)
docker-compose run --rm airflow-init

# 4. Start everything
docker-compose up -d

# 5. Wait ~2 minutes for all services to be healthy
docker-compose ps
```

### Access the UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow** (orchestration) | http://localhost:8080 | admin / admin123 |
| **Metabase** (dashboards) | http://localhost:3000 | (first-run setup) |
| **MinIO** (data lake) | http://localhost:9001 | minioadmin / minioadmin123 |
| **Kafka UI** (monitoring) | http://localhost:8090 | (no auth) |
| **PostgreSQL** | localhost:5432 | jobadmin / jobpassword123 |

### Trigger the pipeline manually

```bash
# Option 1: Via Airflow UI
# → Go to http://localhost:8080 → DAGs → jobs_pipeline → ▶️ Trigger

# Option 2: Via CLI
docker-compose exec airflow-webserver airflow dags trigger jobs_pipeline

# Option 3: Run scrapers directly
docker-compose run --rm scrapers python run_all_scrapers.py
```

---

## 📁 Project Structure

```
moroccan-job-analyzer/
├── docker-compose.yml          ← Launch the full stack
├── .env                        ← Environment variables
├── README.md                   ← This file
├── DATA_DICTIONARY.md          ← Field definitions for all layers
├── LINEAGE.md                  ← Data flow documentation
│
├── scrapers/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── base_scraper.py         ← Abstract base class (MinIO + Kafka)
│   ├── scraper_rekrute.py      ← Rekrute.com scraper
│   ├── scraper_emploi.py       ← Emploi.ma scraper
│   ├── scraper_indeed.py       ← Indeed Morocco scraper
│   ├── kafka_consumer.py       ← Streaming consumer → MinIO
│   └── run_all_scrapers.py     ← Master runner script
│
├── transformations/
│   ├── silver_transform.py     ← Bronze → Silver (cleaning)
│   └── gold_aggregations.py    ← Silver → Gold (analytics)
│
├── airflow/
│   ├── dags/
│   │   └── jobs_pipeline.py    ← Main Airflow DAG
│   ├── Dockerfile
│   └── requirements.txt
│
├── quality/
│   └── data_quality_checks.py  ← Quality checks (all 3 layers)
│
└── warehouse/
    └── init.sql                ← PostgreSQL schema + seed data
```

---

## 🔄 Pipeline Details

### Medallion Architecture

| Layer | Storage | Format | Purpose |
|-------|---------|--------|---------|
| 🥉 **Bronze** | `s3://bronze/` | JSON | Raw, never modified |
| 🥈 **Silver** | `s3://silver/` | JSON/Parquet | Cleaned, normalized |
| 🥇 **Gold** | `s3://gold/` + PostgreSQL | JSON/Parquet + Tables | Aggregated for dashboards |

### Data Sources

| Source | URL | Frequency | Expected Jobs/Run |
|--------|-----|-----------|-------------------|
| Rekrute.com | rekrute.com | Hourly | 50–150 |
| Emploi.ma | emploi.ma | Hourly | 30–100 |
| Indeed Morocco | ma.indeed.com | Hourly | 60–200 |

### Airflow DAG (`jobs_pipeline`)

```
[scrape_all_sources] → [validate_bronze] → [transform_silver]
                                                    ↓
[update_pipeline_log] ← [build_gold] ← [validate_silver]
```

Schedule: `@hourly` | Retries: 3 | Max active runs: 1

---

## 🛠️ Configuration

All configuration is in `.env`. Key variables:

```bash
# Scraping behavior
SCRAPER_MAX_PAGES=5        # Pages per scraper per run
SCRAPER_DELAY=2.0          # Seconds between requests

# MinIO (Data Lake)
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123

# PostgreSQL (Data Warehouse)
POSTGRES_DB=job_market
POSTGRES_USER=jobadmin
POSTGRES_PASSWORD=jobpassword123
```

---

## 📊 Metabase Dashboard Setup

After first launch:

1. Navigate to http://localhost:3000
2. Complete the Metabase setup wizard
3. When asked for a database, connect to PostgreSQL:
   - Host: `postgres`
   - Port: `5432`
   - Database: `job_market`
   - Username: `jobadmin`
   - Password: `jobpassword123`
4. Start creating dashboards using the `dw` schema tables

### Recommended First Queries

```sql
-- Top 10 skills in the last 7 days
SELECT skill, SUM(job_count) AS total
FROM dw.agg_top_skills_daily
WHERE date >= CURRENT_DATE - 7
  AND city = 'all'
GROUP BY skill
ORDER BY total DESC
LIMIT 10;

-- Jobs by city
SELECT city, SUM(job_count) AS total
FROM dw.agg_jobs_by_city
WHERE date >= CURRENT_DATE - 30
GROUP BY city
ORDER BY total DESC;

-- Contract type distribution
SELECT contract_type, SUM(job_count) AS total
FROM dw.agg_contract_distribution
WHERE date >= CURRENT_DATE - 30
GROUP BY contract_type;

-- Pipeline health
SELECT run_id, status, jobs_scraped, jobs_silver, started_at
FROM dw.pipeline_runs
ORDER BY started_at DESC
LIMIT 10;
```

---

## 🔍 Troubleshooting

### Services not starting

```bash
# Check logs
docker-compose logs -f airflow-webserver
docker-compose logs -f minio
docker-compose logs -f postgres

# Restart a specific service
docker-compose restart airflow-scheduler
```

### Scrapers returning 0 jobs

This is normal on first run if job sites have changed their HTML structure.
The scrapers use multiple fallback selectors. Check:

```bash
docker-compose logs scrapers
```

### Airflow DAG not visible

```bash
# Force DAG refresh
docker-compose exec airflow-webserver airflow dags list
```

### Reset everything (⚠️ deletes all data)

```bash
docker-compose down -v
docker-compose up -d
```

---

## 📦 Tech Stack

| Component | Technology | Version |
|-----------|------------|---------|
| Scraping | Python + BeautifulSoup + Scrapy | Python 3.11 |
| Streaming | Apache Kafka | 7.5.0 (Confluent) |
| Data Lake | MinIO | 2024-01 |
| Processing | PySpark / Pandas | Spark 3.5 / Pandas 2.2 |
| Orchestration | Apache Airflow | 2.8.1 |
| Data Warehouse | PostgreSQL | 15 |
| Visualization | Metabase | 0.48.6 |
| Quality | Great Expectations | 0.18 |
| Containers | Docker + Docker Compose | Latest |

---

## 👥 Team & Context

- **Program:** Master IADATA — EMSI Casablanca
- **Academic Year:** 2025/2026
- **Project Deadline:** May 10, 2026
- **Module:** Architecture des Systèmes Big Data

---

## 📄 Documentation

- [`DATA_DICTIONARY.md`](DATA_DICTIONARY.md) — All field definitions
- [`LINEAGE.md`](LINEAGE.md) — Data flow and lineage tracking
- [`VISUALIZATION.md`](VISUALIZATION.md) — Dashboards, data quality controls, and governance evidence
