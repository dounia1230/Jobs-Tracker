"""
============================================================
Moroccan Job Market Analyzer — Silver → Gold Aggregations
============================================================
Reads clean Parquet data from Silver layer and produces
pre-aggregated analytical tables in the Gold layer.

Aggregations produced:
  1. top_skills_daily    — skill demand per day
  2. jobs_by_city        — postings per city
  3. jobs_by_company     — top hiring companies
  4. jobs_by_sector      — industry breakdown
  5. salary_stats        — salary analysis
  6. contract_dist       — CDI/CDD/Stage/Freelance split
  7. remote_jobs         — remote vs on-site ratio
  8. posting_velocity    — new jobs per hour/day

Also loads aggregates into PostgreSQL for Metabase dashboards.
============================================================
"""

import json
import logging
import os
from datetime import datetime, date
from typing import Optional
import io

logger = logging.getLogger(__name__)

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


def _get_minio_client():
    from minio import Minio
    return Minio(
        os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        secure=False
    )


def _get_pg_conn():
    import psycopg2
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "job_market"),
        user=os.getenv("POSTGRES_USER", "jobadmin"),
        password=os.getenv("POSTGRES_PASSWORD", "jobpassword123")
    )


def _dedupe_by_url(jobs: list[dict]) -> list[dict]:
    latest_by_url = {}
    for job in jobs:
        url = job.get("url")
        if not url:
            continue
        current = latest_by_url.get(url)
        if current is None or str(job.get("processed_at") or "") >= str(current.get("processed_at") or ""):
            latest_by_url[url] = job
    return list(latest_by_url.values())


def load_silver_data(pipeline_run_id: Optional[str] = None, dedupe: bool = True) -> list[dict]:
    """Load Silver records from MinIO, optionally scoped to a pipeline run."""
    client = _get_minio_client()
    all_jobs = []
    try:
        objects = list(client.list_objects("silver", recursive=True))
        for obj in objects:
            if not obj.object_name.endswith(".json"):
                continue
            resp = client.get_object("silver", obj.object_name)
            data = json.loads(resp.read().decode("utf-8"))
            resp.close()
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                records = [data]
            else:
                records = []

            for record in records:
                if pipeline_run_id and record.get("pipeline_run_id") != pipeline_run_id:
                    continue
                all_jobs.append(record)
    except Exception as e:
        logger.error(f"Error loading Silver data: {e}")
    return _dedupe_by_url(all_jobs) if dedupe else all_jobs


def _column(df, name: str, default=None):
    if name in df.columns:
        return df[name]
    return pd.Series([default] * len(df), index=df.index)


def compute_gold_aggregates(jobs: list[dict]) -> dict:
    """Compute all Gold aggregates from Silver job records."""
    if not PANDAS_AVAILABLE or not jobs:
        return {}

    df = pd.DataFrame(jobs)
    today = date.today().isoformat()
    updated_at = datetime.utcnow().isoformat()

    # Ensure skills is a list column
    if "skills" in df.columns:
        df["skills"] = df["skills"].apply(
            lambda s: json.loads(s) if isinstance(s, str) else (s or [])
        )
    else:
        df["skills"] = [[] for _ in range(len(df))]

    # Ensure expected columns exist for groupby operations.
    for column, default in {
        "date_posted": today,
        "city": None,
        "company": None,
        "sector": None,
        "contract_type": "unknown",
        "is_remote": False,
        "scraped_at": None,
    }.items():
        if column not in df.columns:
            df[column] = default

    df["date_posted"] = df["date_posted"].fillna(today)

    results = {}

    # ── 1. Top skills per day ──────────────────────────────────
    skills_rows = []
    for _, row in df.iterrows():
        for skill in (row.get("skills") or []):
            skills_rows.append({
                "date": row.get("date_posted", today),
                "skill": skill,
                "city": "all",
                "source": "all",
                "updated_at": updated_at
            })
    if skills_rows:
        skills_df = pd.DataFrame(skills_rows)
        top_skills = (
            skills_df.groupby(["date", "skill", "city", "source"])
            .size().reset_index(name="job_count")
        )
        results["top_skills_daily"] = top_skills.to_dict(orient="records")

    # ── 2. Jobs by city ────────────────────────────────────────
    city_df = (
        df[df["city"].notna()]
        .groupby(["city"])
        .size().reset_index(name="job_count")
        .assign(date=today, source="all", updated_at=updated_at)
    )
    results["jobs_by_city"] = city_df.to_dict(orient="records")

    # ── 3. Jobs by company ────────────────────────────────────
    company_df = (
        df[df["company"].notna()]
        .groupby(["company"])
        .size().reset_index(name="job_count")
        .assign(date=today, updated_at=updated_at)
        .nlargest(50, "job_count")
    )
    results["jobs_by_company"] = company_df.to_dict(orient="records")

    # ── 4. Jobs by sector ─────────────────────────────────────
    sector_df = (
        df[df["sector"].notna()]
        .groupby(["sector"])
        .size().reset_index(name="job_count")
        .assign(date=today, updated_at=updated_at)
    )
    results["jobs_by_sector"] = sector_df.to_dict(orient="records")

    # ── 5. Contract distribution ──────────────────────────────
    contract_df = (
        df[df["contract_type"].notna()]
        .groupby(["contract_type"])
        .size().reset_index(name="job_count")
        .assign(date=today, source="all", updated_at=updated_at)
    )
    results["contract_distribution"] = contract_df.to_dict(orient="records")

    # ── 6. Remote jobs ────────────────────────────────────────
    remote_col = df["is_remote"]
    remote_df = (
        df.assign(is_remote=remote_col.fillna(False))
        .groupby(["is_remote"])
        .size().reset_index(name="job_count")
        .assign(date=today, source="all", updated_at=updated_at)
    )
    results["remote_jobs"] = remote_df.to_dict(orient="records")

    # ── 7. Salary stats ────────────────────────────────────────
    salary_min_col = _column(df, "salary_min")
    salary_max_col = _column(df, "salary_max")
    salary_df = df[salary_min_col.notna() & salary_max_col.notna()].copy()
    if len(salary_df) > 0 and "title" in salary_df.columns:
        salary_stats = (
            salary_df
            .groupby(["title"])
            .agg(
                salary_min=("salary_min", "min"),
                salary_max=("salary_max", "max"),
                salary_avg=("salary_min", "mean"),
                sample_size=("salary_min", "count")
            )
            .reset_index()
            .assign(city="all", sector="all", date=today, updated_at=updated_at)
        )
        results["salary_stats"] = salary_stats.to_dict(orient="records")

    # ── 8. Posting velocity ────────────────────────────────────
    velocity_df = (
        df[df["scraped_at"].notna()]
        .assign(
            bucket_hour=lambda x: pd.to_datetime(x["scraped_at"], errors="coerce").dt.floor("h")
        )
        .dropna(subset=["bucket_hour"])
        .groupby(["bucket_hour"])
        .size().reset_index(name="job_count")
        .assign(source="all", updated_at=updated_at)
    )
    results["posting_velocity"] = velocity_df.to_dict(orient="records")

    return results


def save_gold_to_minio(aggregates: dict) -> None:
    """Save Gold aggregates as JSON to MinIO."""
    client = _get_minio_client()
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    if not client.bucket_exists("gold"):
        client.make_bucket("gold")

    for agg_name, records in aggregates.items():
        if not records:
            continue
        content = json.dumps(records, ensure_ascii=False, default=str).encode("utf-8")
        object_path = f"{agg_name}/{datetime.utcnow().year}/{datetime.utcnow().month:02d}/{agg_name}_{timestamp}.json"
        try:
            client.put_object(
                "gold", object_path,
                io.BytesIO(content), len(content),
                content_type="application/json"
            )
            logger.info(f"Gold saved: s3://gold/{object_path} ({len(records)} records)")
        except Exception as e:
            logger.error(f"Error saving Gold {agg_name}: {e}")


def load_gold_to_postgres(aggregates: dict) -> dict:
    """
    UPSERT Gold aggregates into PostgreSQL data warehouse.
    Returns counts per table.
    """
    conn = _get_pg_conn()
    cur = conn.cursor()
    counts = {}

    try:
        # ── Top skills daily ───────────────────────────────────
        skills = aggregates.get("top_skills_daily", [])
        for row in skills:
            cur.execute("""
                INSERT INTO dw.agg_top_skills_daily (date, skill, job_count, city, source, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (date, skill, city, source) DO UPDATE
                SET job_count = EXCLUDED.job_count, updated_at = EXCLUDED.updated_at
            """, (row["date"], row["skill"], row["job_count"], row["city"], row["source"], row["updated_at"]))
        counts["top_skills_daily"] = len(skills)

        # ── Jobs by city ───────────────────────────────────────
        cities = aggregates.get("jobs_by_city", [])
        for row in cities:
            cur.execute("""
                INSERT INTO dw.agg_jobs_by_city (city, date, job_count, source, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (city, date, source) DO UPDATE
                SET job_count = EXCLUDED.job_count, updated_at = EXCLUDED.updated_at
            """, (row["city"], row["date"], row["job_count"], row.get("source", "all"), row["updated_at"]))
        counts["jobs_by_city"] = len(cities)

        # ── Jobs by company ────────────────────────────────────
        companies = aggregates.get("jobs_by_company", [])
        for row in companies:
            cur.execute("""
                INSERT INTO dw.agg_jobs_by_company (company, date, job_count, updated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (company, date) DO UPDATE
                SET job_count = EXCLUDED.job_count, updated_at = EXCLUDED.updated_at
            """, (row["company"], row["date"], row["job_count"], row["updated_at"]))
        counts["jobs_by_company"] = len(companies)

        sectors = aggregates.get("jobs_by_sector", [])
        for row in sectors:
            cur.execute("""
                INSERT INTO dw.agg_jobs_by_sector (sector, date, job_count, updated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (sector, date) DO UPDATE
                SET job_count = EXCLUDED.job_count, updated_at = EXCLUDED.updated_at
            """, (row["sector"], row["date"], row["job_count"], row["updated_at"]))
        counts["jobs_by_sector"] = len(sectors)

        # ── Contract distribution ──────────────────────────────
        contracts = aggregates.get("contract_distribution", [])
        for row in contracts:
            cur.execute("""
                INSERT INTO dw.agg_contract_distribution (contract_type, date, job_count, source, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (contract_type, date, source) DO UPDATE
                SET job_count = EXCLUDED.job_count, updated_at = EXCLUDED.updated_at
            """, (row["contract_type"], row["date"], row["job_count"], row.get("source", "all"), row["updated_at"]))
        counts["contract_distribution"] = len(contracts)

        # ── Remote jobs ────────────────────────────────────────
        remote = aggregates.get("remote_jobs", [])
        for row in remote:
            cur.execute("""
                INSERT INTO dw.agg_remote_jobs (date, is_remote, job_count, source, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (date, is_remote, source) DO UPDATE
                SET job_count = EXCLUDED.job_count, updated_at = EXCLUDED.updated_at
            """, (row["date"], bool(row["is_remote"]), row["job_count"], row.get("source", "all"), row["updated_at"]))
        counts["remote_jobs"] = len(remote)

        salary = aggregates.get("salary_stats", [])
        for row in salary:
            cur.execute("""
                INSERT INTO dw.agg_salary_stats (
                    title, city, sector, salary_min, salary_max,
                    salary_avg, sample_size, date, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (title, city, date) DO UPDATE
                SET sector = EXCLUDED.sector,
                    salary_min = EXCLUDED.salary_min,
                    salary_max = EXCLUDED.salary_max,
                    salary_avg = EXCLUDED.salary_avg,
                    sample_size = EXCLUDED.sample_size,
                    updated_at = EXCLUDED.updated_at
            """, (
                row["title"], row.get("city", "all"), row.get("sector", "all"),
                row.get("salary_min"), row.get("salary_max"),
                row.get("salary_avg"), row.get("sample_size"),
                row["date"], row["updated_at"]
            ))
        counts["salary_stats"] = len(salary)

        velocity = aggregates.get("posting_velocity", [])
        for row in velocity:
            cur.execute("""
                INSERT INTO dw.agg_posting_velocity (bucket_hour, source, job_count, updated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (bucket_hour, source) DO UPDATE
                SET job_count = EXCLUDED.job_count, updated_at = EXCLUDED.updated_at
            """, (
                row["bucket_hour"], row.get("source", "all"),
                row["job_count"], row["updated_at"]
            ))
        counts["posting_velocity"] = len(velocity)

        conn.commit()
        logger.info(f"Gold data loaded to PostgreSQL: {counts}")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading Gold to PostgreSQL: {e}")
        raise
    finally:
        cur.close()
        conn.close()

    return counts


def upsert_fact_jobs(jobs: list[dict]) -> int:
    """UPSERT Silver job records into PostgreSQL fact_jobs table."""
    conn = _get_pg_conn()
    cur = conn.cursor()
    inserted = 0

    try:
        for job in jobs:
            if job.get("is_low_quality"):
                continue

            salary_min, salary_max = job.get("salary_min"), job.get("salary_max")
            cur.execute("""
                INSERT INTO dw.fact_jobs (
                    title, company, city, sector, contract_type,
                    salary_min, salary_max, date_posted, source, url,
                    language, is_remote, is_low_quality, description,
                    pipeline_run_id, scraped_at, processed_at
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s
                )
                ON CONFLICT (url) DO UPDATE SET
                    title = EXCLUDED.title,
                    company = EXCLUDED.company,
                    city = EXCLUDED.city,
                    sector = EXCLUDED.sector,
                    contract_type = EXCLUDED.contract_type,
                    salary_min = EXCLUDED.salary_min,
                    salary_max = EXCLUDED.salary_max,
                    language = EXCLUDED.language,
                    is_remote = EXCLUDED.is_remote,
                    is_low_quality = EXCLUDED.is_low_quality,
                    description = EXCLUDED.description,
                    pipeline_run_id = EXCLUDED.pipeline_run_id,
                    scraped_at = EXCLUDED.scraped_at,
                    processed_at = COALESCE(EXCLUDED.processed_at, NOW())
                RETURNING job_id
            """, (
                job.get("title"), job.get("company"), job.get("city"),
                job.get("sector"), job.get("contract_type"),
                salary_min, salary_max,
                job.get("date_posted"), job.get("source"), job.get("url"),
                job.get("language", "fr"), bool(job.get("is_remote", False)),
                bool(job.get("is_low_quality", False)), job.get("description"),
                job.get("pipeline_run_id"), job.get("scraped_at"), job.get("processed_at")
            ))

            result = cur.fetchone()
            if result:
                job_id = result[0]
                inserted += 1
                cur.execute("DELETE FROM dw.dim_job_skills WHERE job_id = %s", (job_id,))
                for skill in (job.get("skills") or []):
                    cur.execute("""
                        INSERT INTO dw.dim_job_skills (job_id, skill)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                    """, (job_id, skill))

        conn.commit()
        logger.info(f"Upserted {inserted} fact_jobs records")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error upserting fact_jobs: {e}")
        raise
    finally:
        cur.close()
        conn.close()

    return inserted


def run_gold_aggregations(pipeline_run_id: Optional[str] = None) -> dict:
    """
    Main entry point for Silver → Gold aggregation.
    Called from Airflow DAG or standalone.
    """
    logger.info(f"Starting Gold aggregations (run_id={pipeline_run_id})")

    # 1. Load the current run for fact upserts, and the deduped lake for aggregates.
    current_jobs = load_silver_data(pipeline_run_id=pipeline_run_id)
    jobs = load_silver_data()
    if not current_jobs:
        logger.warning("No Silver data found, skipping Gold aggregations")
        return {}

    logger.info(f"Loaded {len(current_jobs)} current Silver records; {len(jobs)} deduplicated Silver records total")

    # 2. Upsert raw records into PostgreSQL fact table
    inserted = upsert_fact_jobs(current_jobs)
    logger.info(f"Upserted {inserted} records into fact_jobs")

    # 3. Compute aggregations
    aggregates = compute_gold_aggregates(jobs)
    logger.info(f"Computed {len(aggregates)} Gold aggregates")

    # 4. Save to MinIO Gold
    save_gold_to_minio(aggregates)

    # 5. Load into PostgreSQL for Metabase
    counts = load_gold_to_postgres(aggregates)

    logger.info(f"Gold aggregation complete: {counts}")
    return counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = run_gold_aggregations()
    print(f"Gold aggregations: {result}")
