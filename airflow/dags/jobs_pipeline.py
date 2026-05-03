"""
============================================================
Moroccan Job Market Analyzer — Main Airflow DAG
============================================================
DAG: jobs_pipeline
Schedule: every hour
Tasks:
  1. scrape_all_sources   — run all scrapers
  2. validate_bronze      — Great Expectations on Bronze
  3. transform_silver     — Bronze → Silver cleaning
  4. validate_silver      — Great Expectations on Silver
  5. build_gold           — Silver → Gold aggregations
  6. load_warehouse       — Gold → PostgreSQL
  7. refresh_dashboard    — notify Metabase (optional)
============================================================
"""

import sys
import os
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# Add custom modules to Python path
sys.path.insert(0, "/opt/airflow/scrapers")
sys.path.insert(0, "/opt/airflow/transformations")
sys.path.insert(0, "/opt/airflow/quality")

logger = logging.getLogger(__name__)

PIPELINE_TASK_IDS = {
    "scrape_all_sources",
    "validate_bronze",
    "transform_silver",
    "validate_silver",
    "build_gold",
}

# ── Default task arguments ─────────────────────────────────────
default_args = {
    "owner": "job_market_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def _get_pg_conn():
    import psycopg2

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "job_market"),
        user=os.getenv("POSTGRES_USER", "jobadmin"),
        password=os.getenv("POSTGRES_PASSWORD", "jobpassword123")
    )


def _upsert_pipeline_run(run_id: str, started_at, finished_at=None,
                         status: str = "running", jobs_scraped: int = 0,
                         jobs_silver: int = 0, jobs_gold: int = 0,
                         error_msg=None) -> None:
    conn = _get_pg_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dw.pipeline_runs (
            run_id, dag_id, started_at, finished_at, status,
            jobs_scraped, jobs_silver, jobs_gold, error_msg
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (run_id) DO UPDATE SET
            finished_at = EXCLUDED.finished_at,
            status = EXCLUDED.status,
            jobs_scraped = EXCLUDED.jobs_scraped,
            jobs_silver = EXCLUDED.jobs_silver,
            jobs_gold = EXCLUDED.jobs_gold,
            error_msg = EXCLUDED.error_msg
    """, (
        run_id, "jobs_pipeline", started_at, finished_at, status,
        jobs_scraped, jobs_silver, jobs_gold, error_msg
    ))
    conn.commit()
    cur.close()
    conn.close()


# ════════════════════════════════════════════════════════════════
# TASK FUNCTIONS
# ════════════════════════════════════════════════════════════════

def task_scrape_all_sources(**context):
    """
    Task 1: Run all scrapers.
    Pushes scraping summary to XCom for downstream tasks.
    """
    from run_all_scrapers import run_all

    dag_run_id = context["dag_run"].run_id

    logger.info(f"Starting scraping run: {dag_run_id}")
    _upsert_pipeline_run(
        run_id=dag_run_id,
        started_at=context["data_interval_start"],
        status="running"
    )

    summary = run_all(
        sources=["rekrute", "emploi", "indeed"],
        pipeline_run_id=dag_run_id
    )

    total_jobs = sum(info.get("job_count", 0) for info in summary.values())
    failed_sources = [s for s, info in summary.items() if info["status"] == "failed"]

    if failed_sources:
        logger.warning(f"Some scrapers failed: {failed_sources}")

    if total_jobs == 0:
        raise ValueError("No jobs scraped from any source — check site availability")

    # Push summary to XCom
    context["ti"].xcom_push(key="scraping_summary", value=summary)
    context["ti"].xcom_push(key="total_scraped", value=total_jobs)

    logger.info(f"Scraping complete: {total_jobs} jobs from {len(summary)} sources")
    return summary


def task_validate_bronze(**context):
    """
    Task 2: Run data quality checks on Bronze layer.
    Fails if critical checks don't pass.
    """
    from data_quality_checks import run_bronze_checks

    dag_run_id = context["dag_run"].run_id
    results = run_bronze_checks(run_id=dag_run_id)

    context["ti"].xcom_push(key="bronze_quality", value=results)

    failed_critical = [r for r in results if not r["passed"] and r.get("is_critical")]
    if failed_critical:
        raise ValueError(f"Critical Bronze quality checks failed: {failed_critical}")

    logger.info(f"Bronze quality: {sum(1 for r in results if r['passed'])}/{len(results)} checks passed")
    return results


def task_transform_silver(**context):
    """
    Task 3: Transform Bronze → Silver (cleaning & normalization).
    """
    from silver_transform import run_silver_transform

    dag_run_id = context["dag_run"].run_id

    count = run_silver_transform(
        pipeline_run_id=dag_run_id,
        use_spark=False  # Use Pandas inside Docker (PySpark optional)
    )

    if count == 0:
        raise ValueError("Silver transformation produced 0 records")

    context["ti"].xcom_push(key="silver_count", value=count)
    logger.info(f"Silver transformation complete: {count} records")
    return count


def task_validate_silver(**context):
    """
    Task 4: Run data quality checks on Silver layer.
    """
    from data_quality_checks import run_silver_checks

    dag_run_id = context["dag_run"].run_id
    results = run_silver_checks(run_id=dag_run_id)

    context["ti"].xcom_push(key="silver_quality", value=results)

    failed_critical = [r for r in results if not r["passed"] and r.get("is_critical")]
    if failed_critical:
        logger.warning(f"Silver quality issues: {failed_critical}")
        # Don't fail — just warn, we still want Gold to run

    passed = sum(1 for r in results if r["passed"])
    logger.info(f"Silver quality: {passed}/{len(results)} checks passed")
    return results


def task_build_gold(**context):
    """
    Task 5: Build Gold aggregates from Silver data.
    """
    from gold_aggregations import run_gold_aggregations

    dag_run_id = context["dag_run"].run_id
    counts = run_gold_aggregations(pipeline_run_id=dag_run_id)

    context["ti"].xcom_push(key="gold_counts", value=counts)
    logger.info(f"Gold aggregations complete: {counts}")
    return counts


def task_update_pipeline_log(**context):
    """
    Task 6: Write pipeline run metadata to PostgreSQL.
    Captures final stats from upstream XComs.
    """
    dag_run_id = context["dag_run"].run_id
    ti = context["ti"]

    scraping_summary = ti.xcom_pull(task_ids="scrape_all_sources", key="scraping_summary") or {}
    scraped = ti.xcom_pull(task_ids="scrape_all_sources", key="total_scraped") or 0
    silver_count = ti.xcom_pull(task_ids="transform_silver", key="silver_count") or 0
    gold_counts = ti.xcom_pull(task_ids="build_gold", key="gold_counts") or {}
    gold_total = sum(gold_counts.values()) if gold_counts else 0

    task_instances = context["dag_run"].get_task_instances()
    relevant_states = {
        task.task_id: task.state
        for task in task_instances
        if task.task_id in PIPELINE_TASK_IDS
    }
    failed_tasks = [
        task_id for task_id, state in relevant_states.items()
        if state in {"failed", "upstream_failed"}
    ]
    skipped_tasks = [
        task_id for task_id, state in relevant_states.items()
        if state == "skipped"
    ]
    failed_sources = [
        source for source, info in scraping_summary.items()
        if info.get("status") == "failed"
    ]

    if failed_tasks:
        status = "failed"
        error_msg = f"Failed tasks: {', '.join(sorted(failed_tasks))}"
    elif skipped_tasks or failed_sources:
        status = "partial"
        reasons = []
        if skipped_tasks:
            reasons.append(f"skipped tasks: {', '.join(sorted(skipped_tasks))}")
        if failed_sources:
            reasons.append(f"failed sources: {', '.join(sorted(failed_sources))}")
        error_msg = "; ".join(reasons)
    else:
        status = "success"
        error_msg = None

    _upsert_pipeline_run(
        run_id=dag_run_id,
        started_at=context["data_interval_start"],
        finished_at=datetime.utcnow(),
        status=status,
        jobs_scraped=scraped,
        jobs_silver=silver_count,
        jobs_gold=gold_total,
        error_msg=error_msg,
    )

    logger.info(
        f"Pipeline run logged: status={status}, scraped={scraped}, "
        f"silver={silver_count}, gold={gold_total}"
    )


# ════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ════════════════════════════════════════════════════════════════

with DAG(
    dag_id="jobs_pipeline",
    description="Moroccan Job Market Analyzer — Full ETL Pipeline",
    default_args=default_args,
    schedule_interval="@hourly",           # Run every hour
    start_date=days_ago(1),
    catchup=False,                         # Don't backfill past runs
    max_active_runs=1,                     # Only one run at a time
    tags=["job_market", "moroccan", "etl", "medallion"],
    doc_md="""
    ## Moroccan Job Market Analyzer Pipeline

    This DAG orchestrates the complete data pipeline:

    1. **scrape_all_sources** — Runs scrapers for Rekrute, Emploi.ma, and Indeed Morocco
    2. **validate_bronze** — Data quality checks on raw Bronze data
    3. **transform_silver** — Cleaning and normalization (Bronze → Silver)
    4. **validate_silver** — Data quality checks on cleaned Silver data
    5. **build_gold** — Compute analytics aggregates (Silver → Gold)
    6. **update_pipeline_log** — Record pipeline metadata to PostgreSQL

    **Schedule:** Every hour | **Max active runs:** 1

    **Dashboards:** http://localhost:3000 (Metabase)
    **Data Lake:** http://localhost:9001 (MinIO)
    **Airflow:** http://localhost:8080
    """,
) as dag:

    scrape = PythonOperator(
        task_id="scrape_all_sources",
        python_callable=task_scrape_all_sources,
        execution_timeout=timedelta(minutes=30),
        doc_md="Runs all scrapers (Rekrute, Emploi.ma, Indeed) and saves raw data to Bronze.",
    )

    validate_bronze = PythonOperator(
        task_id="validate_bronze",
        python_callable=task_validate_bronze,
        execution_timeout=timedelta(minutes=5),
        doc_md="Great Expectations quality checks on Bronze layer.",
    )

    transform_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=task_transform_silver,
        execution_timeout=timedelta(minutes=20),
        doc_md="Clean and normalize raw data. Output: Silver Parquet in MinIO.",
    )

    validate_silver = PythonOperator(
        task_id="validate_silver",
        python_callable=task_validate_silver,
        execution_timeout=timedelta(minutes=5),
        doc_md="Great Expectations quality checks on Silver layer.",
    )

    build_gold = PythonOperator(
        task_id="build_gold",
        python_callable=task_build_gold,
        execution_timeout=timedelta(minutes=15),
        doc_md="Compute analytics aggregates. Output: Gold tables in MinIO + PostgreSQL.",
    )

    log_run = PythonOperator(
        task_id="update_pipeline_log",
        python_callable=task_update_pipeline_log,
        execution_timeout=timedelta(minutes=2),
        trigger_rule=TriggerRule.ALL_DONE,  # Always run even if upstream fails
        doc_md="Record pipeline run metadata and stats to pipeline_runs table.",
    )

    # ── Task dependencies ──────────────────────────────────────
    # scrape → validate_bronze → transform_silver → validate_silver → build_gold → log_run
    scrape >> validate_bronze >> transform_silver >> validate_silver >> build_gold >> log_run
