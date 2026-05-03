# Moroccan Job Market Analyzer — Data Lineage

> **Platform:** EMSI IADATA 2025/2026 | **Team:** Architecture Données

---

## Data Flow Overview

```
╔══════════════════════════════════════════════════════════════════════╗
║                    DATA SOURCES (External)                          ║
║  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 ║
║  │ Rekrute.com │  │ Emploi.ma   │  │ Indeed (MA) │                 ║
║  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                 ║
╚═════════╪════════════════╪════════════════╪════════════════════════╝
          │                │                │
          ▼                ▼                ▼
╔══════════════════════════════════════════════════════════════════════╗
║                    INGESTION LAYER                                  ║
║                                                                      ║
║  ┌────────────────────────────────────┐                             ║
║  │  Python Scrapers (BeautifulSoup)   │  ← Run every hour via      ║
║  │  scraper_rekrute.py                │    Apache Airflow DAG       ║
║  │  scraper_emploi.py                 │                             ║
║  │  scraper_indeed.py                 │                             ║
║  └──────────────┬─────────────────────┘                             ║
║                 │                                                    ║
║         ┌───────┴──────────┐                                        ║
║         ▼                  ▼                                        ║
║  ┌─────────────┐   ┌──────────────────┐                            ║
║  │ Batch Save  │   │  Kafka Producer  │  topic: jobs-raw           ║
║  │ to MinIO    │   │  (per-job event) │                             ║
║  └─────────────┘   └──────────────────┘                            ║
╚══════════════════════════════════════════════════════════════════════╝
          │                  │
          ▼                  ▼
╔══════════════════════════════════════════════════════════════════════╗
║                    🥉 BRONZE LAYER (Raw)                            ║
║                                                                      ║
║  MinIO bucket: s3://bronze/                                         ║
║  ┌─────────────────────────────────────────────────────────────┐   ║
║  │  bronze/                                                     │   ║
║  │  ├── rekrute/2025/04/25/rekrute_20250425_140000.json        │   ║
║  │  ├── emploi/2025/04/25/emploi_20250425_140500.json          │   ║
║  │  └── indeed/2025/04/25/indeed_20250425_141000.json          │   ║
║  └─────────────────────────────────────────────────────────────┘   ║
║                                                                      ║
║  Format: JSON (raw strings, no validation)                          ║
║  Retention: Indefinite (append-only)                                ║
║  Schema: uid, title, company, city, contract_type, salary,          ║
║          date_posted, url, description, source, scraped_at,         ║
║          pipeline_run_id                                            ║
╚══════════════════════════════════════════════════════════════════════╝
          │
          ▼
╔══════════════════════════════════════════════════════════════════════╗
║              🔍 DATA QUALITY — BRONZE CHECKS                        ║
║  Great Expectations / custom validators                             ║
║  • File non-empty        • Has scraped_at                           ║
║  • Min record count      • Has URL                                  ║
║  Results → dw.quality_reports                                       ║
╚══════════════════════════════════════════════════════════════════════╝
          │
          ▼
╔══════════════════════════════════════════════════════════════════════╗
║               🥈 SILVER LAYER (Cleaned)                             ║
║                                                                      ║
║  silver_transform.py  (PySpark / Pandas)                            ║
║  ┌─────────────────────────────────────────────────────────────┐   ║
║  │  Transformations:                                            │   ║
║  │  1. Strip HTML from title + description                     │   ║
║  │  2. Normalize city names (aliases → canonical)              │   ║
║  │  3. Lowercase + trim all text fields                        │   ║
║  │  4. Parse dates → ISO 8601 (YYYY-MM-DD)                    │   ║
║  │  5. Infer contract_type from text                           │   ║
║  │  6. Detect language (fr/ar/en)                              │   ║
║  │  7. Extract skills via keyword matching                     │   ║
║  │  8. Detect remote work mentions                             │   ║
║  │  9. Parse salary → (salary_min, salary_max) in MAD         │   ║
║  │ 10. Flag low quality records                                │   ║
║  │ 11. Deduplicate by URL                                      │   ║
║  └─────────────────────────────────────────────────────────────┘   ║
║                                                                      ║
║  MinIO bucket: s3://silver/                                         ║
║  ├── silver/jobs/2025/04/silver_20250425_141500.json               ║
║  Format: JSON (Parquet in PySpark mode)                             ║
║  Added fields: skills[], language, is_remote, salary_min/max,       ║
║                is_low_quality, processed_at                         ║
╚══════════════════════════════════════════════════════════════════════╝
          │
          ▼
╔══════════════════════════════════════════════════════════════════════╗
║              🔍 DATA QUALITY — SILVER CHECKS                        ║
║  • title not null        • URL uniqueness                           ║
║  • date format valid     • contract_type valid set                  ║
║  • description > 50 chars • low quality rate < 20%                  ║
║  Results → dw.quality_reports                                       ║
╚══════════════════════════════════════════════════════════════════════╝
          │
          ▼
╔══════════════════════════════════════════════════════════════════════╗
║                    🥇 GOLD LAYER (Analytical)                       ║
║                                                                      ║
║  gold_aggregations.py  (Pandas + PostgreSQL UPSERT)                 ║
║  ┌─────────────────────────────────────────────────────────────┐   ║
║  │  Aggregates computed:                                        │   ║
║  │  • top_skills_daily   — skill demand per day                │   ║
║  │  • jobs_by_city       — postings per city                   │   ║
║  │  • jobs_by_company    — top hiring companies                │   ║
║  │  • jobs_by_sector     — industry breakdown                  │   ║
║  │  • contract_distribution — CDI/CDD/Stage/Freelance          │   ║
║  │  • remote_jobs        — remote vs on-site                   │   ║
║  │  • salary_stats       — salary by title                     │   ║
║  │  • posting_velocity   — jobs per hour                       │   ║
║  └─────────────────────────────────────────────────────────────┘   ║
║                                                                      ║
║  MinIO bucket: s3://gold/                                           ║
║  Format: JSON (Parquet in PySpark mode)                             ║
╚══════════════════════════════════════════════════════════════════════╝
          │
          ▼
╔══════════════════════════════════════════════════════════════════════╗
║                  DATA WAREHOUSE (PostgreSQL)                        ║
║                                                                      ║
║  Database: job_market | Schema: dw                                  ║
║  ┌───────────────────────────────────────────────────────────┐     ║
║  │  dw.fact_jobs          ← 1 row per unique job posting     │     ║
║  │  dw.dim_job_skills     ← skills per job                   │     ║
║  │  dw.agg_top_skills_daily                                  │     ║
║  │  dw.agg_jobs_by_city                                      │     ║
║  │  dw.agg_jobs_by_company                                   │     ║
║  │  dw.agg_contract_distribution                             │     ║
║  │  dw.agg_remote_jobs                                       │     ║
║  │  dw.agg_salary_stats                                      │     ║
║  │  dw.pipeline_runs      ← lineage + audit log              │     ║
║  │  dw.quality_reports    ← quality check results            │     ║
║  └───────────────────────────────────────────────────────────┘     ║
╚══════════════════════════════════════════════════════════════════════╝
          │
          ▼
╔══════════════════════════════════════════════════════════════════════╗
║                  VISUALIZATION (Metabase)                           ║
║                                                                      ║
║  Dashboard 1: Job Market Overview (counters, line charts)           ║
║  Dashboard 2: Skills Demand (bar charts, trend lines)               ║
║  Dashboard 3: Geographic Analysis (city breakdowns)                 ║
║  Dashboard 4: Company & Sector (tables, donut charts)               ║
║  Dashboard 5: Salary Insights (box plots, tables)                   ║
║                                                                      ║
║  URL: http://localhost:3000                                         ║
╚══════════════════════════════════════════════════════════════════════╝
```

---

## Record Metadata Fields (Lineage Tracking)

Every record carries the following metadata through all layers:

| Field | Set at | Description |
|-------|--------|-------------|
| `uid` | Scraping | MD5(URL) — stable ID across all runs |
| `source` | Scraping | Which platform produced this record |
| `scraped_at` | Scraping | UTC timestamp of collection |
| `pipeline_run_id` | Airflow | Which DAG run processed this record |
| `processed_at` | Silver | When the Silver transformation ran |

### Tracing a Record End-to-End

Given a `job_id` from `dw.fact_jobs`, you can trace its full history:

```sql
-- Find the original pipeline run that produced this record
SELECT
    j.job_id, j.url, j.source, j.scraped_at,
    j.processed_at, j.pipeline_run_id,
    p.started_at AS dag_started,
    p.finished_at AS dag_finished,
    p.status AS dag_status
FROM dw.fact_jobs j
LEFT JOIN dw.pipeline_runs p ON j.pipeline_run_id = p.run_id
WHERE j.job_id = 42;
```

```sql
-- Quality checks for the run that produced a record
SELECT qr.*
FROM dw.fact_jobs j
JOIN dw.quality_reports qr ON j.pipeline_run_id = qr.run_id
WHERE j.job_id = 42
ORDER BY qr.layer, qr.created_at;
```

---

## Airflow DAG Dependency Graph

```
[scrape_all_sources]
        │
        ▼
[validate_bronze]
        │
        ▼
[transform_silver]
        │
        ▼
[validate_silver]
        │
        ▼
[build_gold]
        │
        ▼
[update_pipeline_log]  ← trigger_rule=all_done (always runs)
```

**Schedule:** `@hourly` (every 60 minutes)
**Retries per task:** 3 with exponential backoff (5min → 30min max)
**Max active runs:** 1 (prevents overlapping pipeline runs)
