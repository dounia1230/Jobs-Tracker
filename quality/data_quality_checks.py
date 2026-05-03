"""
============================================================
Moroccan Job Market Analyzer — Data Quality Checks
============================================================
Implements Great Expectations-style checks at all three
Medallion layers (Bronze, Silver, Gold).

Checks per layer:
  Bronze: file non-empty, has scraped_at, min record count
  Silver: null title/url, URL uniqueness, date format, contract types
  Gold:   positive counts, no future dates, no negative salaries

Results are stored in PostgreSQL quality_reports table.
============================================================
"""

import json
import logging
import os
import re
from datetime import datetime, date
from typing import Optional

logger = logging.getLogger(__name__)


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


def _make_result(check_name: str, passed: bool, details: str,
                 layer: str, is_critical: bool = False) -> dict:
    return {
        "check_name": check_name,
        "passed": passed,
        "details": details,
        "layer": layer,
        "is_critical": is_critical,
        "checked_at": datetime.utcnow().isoformat()
    }


def _save_results_to_pg(results: list[dict], run_id: Optional[str]) -> None:
    """Persist quality check results to PostgreSQL."""
    if not run_id:
        return
    try:
        conn = _get_pg_conn()
        cur = conn.cursor()
        for r in results:
            cur.execute("""
                INSERT INTO dw.quality_reports (run_id, layer, check_name, passed, details)
                VALUES (%s, %s, %s, %s, %s)
            """, (run_id, r["layer"], r["check_name"], r["passed"], r["details"]))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"Could not save quality results to PG: {e}")


def _records_for_run(records: list[dict], run_id: Optional[str]) -> list[dict]:
    if not run_id:
        return records
    return [record for record in records if record.get("pipeline_run_id") == run_id]


# ════════════════════════════════════════════════════════════════
# BRONZE LAYER CHECKS
# ════════════════════════════════════════════════════════════════

def run_bronze_checks(run_id: Optional[str] = None) -> list[dict]:
    """
    Bronze layer quality checks.
    Validates raw JSON files just written to MinIO.
    """
    results = []
    client = _get_minio_client()

    try:
        objects = list(client.list_objects("bronze", recursive=True))
    except Exception as e:
        results.append(_make_result(
            "bronze_bucket_accessible", False,
            f"Cannot access bronze bucket: {e}", "bronze", is_critical=True
        ))
        _save_results_to_pg(results, run_id)
        return results

    # ── Check 1: Bronze bucket has files ──────────────────────
    results.append(_make_result(
        "bronze_has_files",
        len(objects) > 0,
        f"Found {len(objects)} objects in bronze bucket",
        "bronze", is_critical=True
    ))

    all_jobs = []
    json_files = [o for o in objects if o.object_name.endswith(".json")]

    for obj in json_files:
        try:
            resp = client.get_object("bronze", obj.object_name)
            content = resp.read()
            resp.close()

            # ── Check 2: Files are not empty ──────────────────
            results.append(_make_result(
                f"bronze_file_not_empty_{obj.object_name.split('/')[-1]}",
                len(content) > 10,
                f"File size: {len(content)} bytes",
                "bronze"
            ))

            data = json.loads(content.decode("utf-8"))
            if isinstance(data, list):
                all_jobs.extend(_records_for_run(data, run_id))
            elif isinstance(data, dict):
                all_jobs.extend(_records_for_run([data], run_id))

        except Exception as e:
            results.append(_make_result(
                f"bronze_file_parseable_{obj.object_name.split('/')[-1]}",
                False, str(e), "bronze"
            ))

    # ── Check 3: Minimum record count ─────────────────────────
    results.append(_make_result(
        "bronze_min_record_count",
        len(all_jobs) >= 1,
        f"Total Bronze records: {len(all_jobs)}",
        "bronze", is_critical=True
    ))

    # ── Check 4: All records have scraped_at ──────────────────
    missing_ts = sum(1 for j in all_jobs if not j.get("scraped_at"))
    results.append(_make_result(
        "bronze_has_scraped_at",
        missing_ts == 0,
        f"Records missing scraped_at: {missing_ts}/{len(all_jobs)}",
        "bronze"
    ))

    # ── Check 5: All records have URL ─────────────────────────
    missing_url = sum(1 for j in all_jobs if not j.get("url"))
    results.append(_make_result(
        "bronze_has_url",
        missing_url == 0,
        f"Records missing URL: {missing_url}/{len(all_jobs)}",
        "bronze"
    ))

    passed = sum(1 for r in results if r["passed"])
    logger.info(f"Bronze quality: {passed}/{len(results)} checks passed")

    _save_results_to_pg(results, run_id)
    return results


# ════════════════════════════════════════════════════════════════
# SILVER LAYER CHECKS
# ════════════════════════════════════════════════════════════════

def run_silver_checks(run_id: Optional[str] = None) -> list[dict]:
    """
    Silver layer quality checks.
    Validates cleaned data written to MinIO silver bucket.
    """
    results = []
    client = _get_minio_client()

    # Load Silver records
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
                all_jobs.extend(_records_for_run(data, run_id))
            elif isinstance(data, dict):
                all_jobs.extend(_records_for_run([data], run_id))
    except Exception as e:
        results.append(_make_result(
            "silver_bucket_accessible", False, str(e), "silver", is_critical=True
        ))
        _save_results_to_pg(results, run_id)
        return results

    n = len(all_jobs)
    results.append(_make_result(
        "silver_has_records",
        n > 0,
        f"Silver record count: {n}",
        "silver", is_critical=True
    ))
    if n == 0:
        _save_results_to_pg(results, run_id)
        return results

    # ── Check: title not null ──────────────────────────────────
    null_title = sum(1 for j in all_jobs if not j.get("title"))
    results.append(_make_result(
        "silver_title_not_null",
        null_title == 0,
        f"Dimension=complétude | Records with null title: {null_title}/{n} ({null_title/n*100:.1f}%)",
        "silver", is_critical=True
    ))

    # ── Check: URL not null ────────────────────────────────────
    null_url = sum(1 for j in all_jobs if not j.get("url"))
    results.append(_make_result(
        "silver_url_not_null",
        null_url == 0,
        f"Dimension=complétude | Records with null URL: {null_url}/{n}",
        "silver", is_critical=True
    ))

    # ── Check: URL uniqueness ──────────────────────────────────
    urls = [j.get("url") for j in all_jobs if j.get("url")]
    duplicate_urls = len(urls) - len(set(urls))
    results.append(_make_result(
        "silver_url_unique",
        duplicate_urls == 0,
        f"Dimension=cohérence | Duplicate URLs: {duplicate_urls}",
        "silver"
    ))

    # ── Check: title length 5–200 ──────────────────────────────
    bad_title_len = sum(
        1 for j in all_jobs
        if j.get("title") and not (5 <= len(j["title"]) <= 200)
    )
    results.append(_make_result(
        "silver_title_length_valid",
        bad_title_len == 0,
        f"Records with title outside [5,200] chars: {bad_title_len}",
        "silver"
    ))

    # ── Check: description length > 50 ────────────────────────
    short_desc = sum(1 for j in all_jobs if len(j.get("description") or "") < 50)
    results.append(_make_result(
        "silver_description_sufficient",
        short_desc / n < 0.3,  # Allow up to 30% short descriptions
        f"Dimension=validité | Records with missing/short description < 50 chars: {short_desc}/{n}",
        "silver"
    ))

    # ── Check: contract_type is valid ─────────────────────────
    valid_contracts = {"CDI", "CDD", "Stage", "Freelance", "unknown"}
    invalid_contracts = sum(
        1 for j in all_jobs
        if j.get("contract_type") and j["contract_type"] not in valid_contracts
    )
    results.append(_make_result(
        "silver_contract_type_valid",
        invalid_contracts == 0,
        f"Dimension=validité | Records with invalid contract_type: {invalid_contracts}",
        "silver"
    ))

    missing_dates = sum(1 for j in all_jobs if not j.get("date_posted"))
    results.append(_make_result(
        "silver_date_not_null",
        missing_dates / n < 0.2,
        f"Dimension=complétude | Records missing date_posted: {missing_dates}/{n} ({missing_dates/n*100:.1f}%)",
        "silver"
    ))

    # ── Check: date_posted format ─────────────────────────────
    date_regex = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    bad_dates = sum(
        1 for j in all_jobs
        if j.get("date_posted") and not date_regex.match(str(j["date_posted"]))
    )
    results.append(_make_result(
        "silver_date_format_valid",
        bad_dates == 0,
        f"Dimension=validité | Records with invalid date format: {bad_dates}",
        "silver"
    ))

    # ── Check: low_quality rate < 20% ─────────────────────────
    low_quality = sum(1 for j in all_jobs if j.get("is_low_quality"))
    results.append(_make_result(
        "silver_low_quality_rate",
        low_quality / n < 0.2,
        f"Low quality records: {low_quality}/{n} ({low_quality/n*100:.1f}%)",
        "silver"
    ))

    # ── Check: completeness metrics ────────────────────────────
    for field in ["city", "company", "sector"]:
        non_null = sum(1 for j in all_jobs if j.get(field))
        completeness = non_null / n * 100
        results.append(_make_result(
            f"silver_completeness_{field}",
            completeness > 30,  # At least 30% should have this field
            f"Dimension=complétude | Completeness of '{field}': {completeness:.1f}%",
            "silver"
        ))

    passed = sum(1 for r in results if r["passed"])
    logger.info(f"Silver quality: {passed}/{len(results)} checks passed")

    _save_results_to_pg(results, run_id)
    return results


# ════════════════════════════════════════════════════════════════
# GOLD LAYER CHECKS
# ════════════════════════════════════════════════════════════════

def run_gold_checks(run_id: Optional[str] = None) -> list[dict]:
    """
    Gold layer quality checks.
    Validates aggregate tables in PostgreSQL.
    """
    results = []

    try:
        conn = _get_pg_conn()
        cur = conn.cursor()

        # ── Check: agg_top_skills_daily has records ────────────
        cur.execute("SELECT COUNT(*) FROM dw.agg_top_skills_daily")
        count = cur.fetchone()[0]
        results.append(_make_result(
            "gold_skills_has_records",
            count > 0,
            f"Top skills records: {count}",
            "gold", is_critical=True
        ))

        # ── Check: No negative job counts ─────────────────────
        cur.execute("SELECT COUNT(*) FROM dw.agg_top_skills_daily WHERE job_count < 0")
        neg = cur.fetchone()[0]
        results.append(_make_result(
            "gold_no_negative_counts",
            neg == 0,
            f"Negative job_count rows: {neg}",
            "gold"
        ))

        # ── Check: fact_jobs has records ───────────────────────
        cur.execute("SELECT COUNT(*) FROM dw.fact_jobs")
        fact_count = cur.fetchone()[0]
        results.append(_make_result(
            "gold_fact_jobs_has_records",
            fact_count > 0,
            f"fact_jobs record count: {fact_count}",
            "gold", is_critical=True
        ))

        # ── Check: No future date_posted ──────────────────────
        cur.execute("SELECT COUNT(*) FROM dw.fact_jobs WHERE date_posted > CURRENT_DATE")
        future_dates = cur.fetchone()[0]
        results.append(_make_result(
            "gold_no_future_dates",
            future_dates == 0,
            f"Records with future date_posted: {future_dates}",
            "gold"
        ))

        # ── Check: No negative salaries ───────────────────────
        cur.execute("SELECT COUNT(*) FROM dw.fact_jobs WHERE salary_min < 0 OR salary_max < 0")
        neg_salary = cur.fetchone()[0]
        results.append(_make_result(
            "gold_no_negative_salaries",
            neg_salary == 0,
            f"Records with negative salary: {neg_salary}",
            "gold"
        ))

        # ── Check: city distribution ───────────────────────────
        cur.execute("SELECT COUNT(DISTINCT city) FROM dw.fact_jobs WHERE city IS NOT NULL")
        city_count = cur.fetchone()[0]
        results.append(_make_result(
            "gold_city_diversity",
            city_count >= 3,
            f"Distinct cities in fact_jobs: {city_count}",
            "gold"
        ))

        cur.close()
        conn.close()

    except Exception as e:
        results.append(_make_result(
            "gold_pg_accessible", False,
            f"Cannot query PostgreSQL: {e}", "gold", is_critical=True
        ))

    passed = sum(1 for r in results if r["passed"])
    logger.info(f"Gold quality: {passed}/{len(results)} checks passed")

    _save_results_to_pg(results, run_id)
    return results


def generate_quality_report(run_id: Optional[str] = None) -> dict:
    """
    Run all quality checks and generate a full report.
    """
    logger.info("Running full data quality report...")

    report = {
        "run_id": run_id,
        "generated_at": datetime.utcnow().isoformat(),
        "bronze": run_bronze_checks(run_id),
        "silver": run_silver_checks(run_id),
        "gold": run_gold_checks(run_id),
    }

    # Summary
    all_results = report["bronze"] + report["silver"] + report["gold"]
    total = len(all_results)
    passed = sum(1 for r in all_results if r["passed"])
    critical_failed = [r for r in all_results if not r["passed"] and r.get("is_critical")]

    report["summary"] = {
        "total_checks": total,
        "passed": passed,
        "failed": total - passed,
        "pass_rate": f"{passed/total*100:.1f}%" if total > 0 else "0%",
        "critical_failures": len(critical_failed),
    }

    logger.info(
        f"Quality report: {passed}/{total} checks passed "
        f"({report['summary']['pass_rate']}), "
        f"{len(critical_failed)} critical failures"
    )
    return report


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    report = generate_quality_report(run_id="manual_quality_check")
    print(json.dumps(report["summary"], indent=2))
