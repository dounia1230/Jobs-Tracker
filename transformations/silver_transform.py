"""
============================================================
Moroccan Job Market Analyzer — Bronze → Silver Transformation
============================================================
Reads raw JSON from MinIO Bronze, applies cleaning/normalization,
writes clean data to MinIO Silver.

Transformations:
  1. Strip HTML from description and title
  2. Normalize city names (aliases → canonical)
  3. Lowercase + strip whitespace on text fields
  4. Parse dates to ISO 8601 (YYYY-MM-DD)
  5. Deduplicate by URL
  6. Detect language (fr/ar/en)
  7. Infer contract type from text
  8. Extract skills via keyword matching
  9. Detect remote work
 10. Parse salary into (min, max) MAD
 11. Flag low quality records
============================================================
"""

import json
import logging
import os
import re
import io
from datetime import datetime, date
from typing import Optional

logger = logging.getLogger(__name__)

TEXT_FIELDS = (
    "title",
    "company",
    "sector",
    "salary",
    "description",
    "experience_level",
    "education_level",
)

# ── City normalization ─────────────────────────────────────────
CITY_ALIASES = {
    "casa": "Casablanca", "casablanca": "Casablanca",
    "grand casablanca": "Casablanca", "casablanca-settat": "Casablanca",
    "dar el beida": "Casablanca", "ain sebaa": "Casablanca",
    "rabat": "Rabat", "rabat-salé": "Rabat", "rabat-sale": "Rabat",
    "salé": "Rabat", "sale": "Rabat",
    "marrakech": "Marrakech", "marrakesh": "Marrakech",
    "marrakech-safi": "Marrakech",
    "fès": "Fès", "fez": "Fès", "fes": "Fès", "fès-meknès": "Fès",
    "tanger": "Tanger", "tangier": "Tanger",
    "tanger-tétouan": "Tanger", "tanger-tetouan": "Tanger",
    "agadir": "Agadir", "souss-massa": "Agadir",
    "oujda": "Oujda", "oriental": "Oujda",
    "meknès": "Meknès", "meknes": "Meknès",
    "kénitra": "Kénitra", "kenitra": "Kénitra",
    "tétouan": "Tétouan", "tetouan": "Tétouan",
    "télétravail": "Remote", "teletravail": "Remote",
    "remote": "Remote", "à distance": "Remote",
}

CONTRACT_PATTERNS = [
    (re.compile(r"\bcdi\b", re.IGNORECASE), "CDI"),
    (re.compile(r"\bcdd\b", re.IGNORECASE), "CDD"),
    (re.compile(r"\bstage\b|\binternship\b|\bstagiaire\b|\balternance\b", re.IGNORECASE), "Stage"),
    (re.compile(r"\bfreelance\b|\bindépendant\b|\bconsultant\b", re.IGNORECASE), "Freelance"),
    (re.compile(r"\btemporaire\b|\bintérim\b|\binterim\b", re.IGNORECASE), "CDD"),
]

REMOTE_RE = re.compile(
    r"\b(télétravail|teletravail|remote|à distance|home office|hybride|hybrid)\b",
    re.IGNORECASE
)

SKILLS_KEYWORDS = {
    "Python": ["python", "django", "flask", "fastapi", "pandas", "numpy"],
    "SQL": ["sql", "mysql", "postgresql", "oracle sql", "pl/sql", "mariadb"],
    "Java": ["java", "spring boot", "hibernate", "jvm"],
    "JavaScript": ["javascript", " js ", "nodejs", "node.js", "typescript", "express"],
    "PHP": ["php", "laravel", "symfony", "wordpress"],
    "C#": ["c#", "csharp", ".net", "dotnet", "asp.net"],
    "C++": ["c++", "cpp"],
    "React": ["react", "reactjs", "redux", "next.js"],
    "Angular": ["angular", "angularjs"],
    "Vue.js": ["vue.js", "vuejs"],
    "Docker": ["docker", "dockerfile", "docker-compose"],
    "Kubernetes": ["kubernetes", "k8s", "kubectl"],
    "Git": ["git", "github", "gitlab", "bitbucket"],
    "AWS": ["aws", "amazon web services", "ec2", " s3 ", "lambda"],
    "Azure": ["azure", "microsoft azure", "azure devops"],
    "GCP": ["gcp", "google cloud", "bigquery"],
    "Apache Spark": ["spark", "pyspark", "databricks"],
    "Kafka": ["kafka", "apache kafka", "confluent"],
    "Airflow": ["airflow", "apache airflow", " dag "],
    "Power BI": ["power bi", "powerbi"],
    "Tableau": ["tableau"],
    "Machine Learning": ["machine learning", " ml ", "classification", "regression", "sklearn"],
    "Deep Learning": ["deep learning", "neural network", "cnn", "lstm", "transformer"],
    "TensorFlow": ["tensorflow", "keras"],
    "PyTorch": ["pytorch", "torch"],
    "Linux": ["linux", "ubuntu", "centos", "bash", "shell scripting"],
    "Agile": ["agile", "scrum", "kanban", "sprint"],
    "MongoDB": ["mongodb", "mongo"],
    "Redis": ["redis"],
    "Elasticsearch": ["elasticsearch", "elk stack", "kibana"],
    "PostgreSQL": ["postgresql", "postgres"],
    "REST API": ["rest api", "restful", "api rest", "swagger"],
    "Microservices": ["microservices", "microservice"],
    "CI/CD": ["ci/cd", "jenkins", "gitlab ci", "github actions"],
    "Hadoop": ["hadoop", "hdfs", "hive", "mapreduce"],
}


def normalize_city(city: Optional[str]) -> Optional[str]:
    if not city:
        return None
    c = re.sub(r"\(.*?\)", "", city).strip().lower()
    return CITY_ALIASES.get(c, city.strip().title())


def strip_html(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    clean = re.sub(r"<[^>]+>", " ", text)
    clean = re.sub(r"&[a-zA-Z]+;|&#\d+;", " ", clean)
    return re.sub(r"\s+", " ", clean).strip() or None


def clean_text(text: Optional[str], lowercase: bool = True) -> Optional[str]:
    clean = strip_html(text)
    if clean is None:
        return None
    return clean.lower() if lowercase else clean


def parse_date(date_str: Optional[str]) -> Optional[str]:
    if not date_str:
        return None
    date_str = str(date_str).strip()
    today = datetime.utcnow().date()
    from datetime import timedelta

    rel = re.search(r"(\d+)\s*(jour|day|heure|hour|semaine|week|mois|month)", date_str.lower())
    if rel:
        n, unit = int(rel.group(1)), rel.group(2)
        if "jour" in unit or "day" in unit:
            return (today - timedelta(days=n)).isoformat()
        if "heure" in unit or "hour" in unit:
            return today.isoformat()
        if "semaine" in unit or "week" in unit:
            return (today - timedelta(weeks=n)).isoformat()
        if "mois" in unit or "month" in unit:
            return (today - timedelta(days=n * 30)).isoformat()

    for fmt in ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%y",
                "%d %B %Y", "%d %b %Y", "%B %d, %Y", "%Y/%m/%d"]:
        try:
            return datetime.strptime(date_str, fmt).date().isoformat()
        except ValueError:
            pass

    m = re.match(r"(\d{4}-\d{2}-\d{2})", date_str)
    return m.group(1) if m else None


def infer_contract_type(text: Optional[str], field: Optional[str]) -> str:
    for src in [field, text]:
        if src:
            for pat, ct in CONTRACT_PATTERNS:
                if pat.search(src):
                    return ct
    return "unknown"


def extract_skills(text: Optional[str]) -> list:
    if not text:
        return []
    tl = text.lower()
    return list({
        skill for skill, kws in SKILLS_KEYWORDS.items()
        if any(kw in tl for kw in kws)
    })


def detect_language(text: Optional[str]) -> str:
    if not text:
        return "fr"
    arabic_count = sum(1 for c in text if "\u0600" <= c <= "\u06ff")
    if arabic_count > len(text) * 0.3:
        return "ar"
    tl = text.lower()
    en_score = sum(1 for w in ["we are", "you will", "requirements", "skills", "experience"] if w in tl)
    fr_score = sum(1 for w in ["nous", "vous", "profil", "poste", "entreprise", "compétences"] if w in tl)
    return "en" if en_score > fr_score and en_score >= 2 else "fr"


def detect_remote(text: Optional[str], city: Optional[str]) -> bool:
    return bool(REMOTE_RE.search(f"{text or ''} {city or ''}"))


def parse_salary(salary_str: Optional[str]) -> tuple:
    if not salary_str:
        return None, None
    cleaned = re.sub(
        r"\b(mad|dh|dhs|dirham|dirhams)\b|€|\$",
        "",
        salary_str,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"[\s,.]", "", cleaned)
    cleaned = re.sub(r"(\d+)[Kk]", lambda m: str(int(m.group(1)) * 1000), cleaned)
    numbers = [int(n) for n in re.findall(r"\d+", cleaned) if 1000 <= int(n) <= 200_000]
    if not numbers:
        return None, None
    return min(numbers), max(numbers)


def is_low_quality(job: dict) -> bool:
    return (
        not job.get("title") or
        not job.get("url") or
        len(job.get("description") or "") < 50
    )


def transform_job(job: dict, processed_at: str) -> dict:
    """Apply all Silver transformations to a single job dict."""
    cleaned = dict(job)
    for field in TEXT_FIELDS:
        if field in cleaned:
            cleaned[field] = clean_text(cleaned.get(field), lowercase=True)

    desc = cleaned.get("description")
    title = cleaned.get("title")
    search_text = " ".join(part for part in [title, desc] if part)

    salary_min, salary_max = parse_salary(job.get("salary"))

    return {
        **cleaned,
        "title": title,
        "description": desc,
        "city": normalize_city(job.get("city")),
        "date_posted": parse_date(job.get("date_posted")),
        "contract_type": infer_contract_type(search_text, job.get("contract_type")),
        "language": detect_language(search_text),
        "skills": extract_skills(search_text),
        "is_remote": detect_remote(search_text, job.get("city")),
        "salary_min": salary_min,
        "salary_max": salary_max,
        "is_low_quality": False,  # Will be set below
        "processed_at": processed_at,
    }


def _load_bronze_jobs(client, pipeline_run_id: Optional[str]) -> list[dict]:
    raw_jobs = []
    objects = list(client.list_objects("bronze", recursive=True))
    logger.info(f"Found {len(objects)} objects in Bronze")

    for obj in objects:
        if not obj.object_name.endswith(".json"):
            continue
        try:
            resp = client.get_object("bronze", obj.object_name)
            data = json.loads(resp.read().decode("utf-8"))
            resp.close()
            resp.release_conn()
            records = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
            for record in records:
                if pipeline_run_id and record.get("pipeline_run_id") != pipeline_run_id:
                    continue
                raw_jobs.append(record)
        except Exception as e:
            logger.error(f"Error reading {obj.object_name}: {e}")

    return raw_jobs


def run_silver_transform(pipeline_run_id: Optional[str] = None, use_spark: bool = False) -> int:
    """
    Main entry point for Bronze → Silver transformation.
    Reads all Bronze JSON files from MinIO, transforms them,
    and writes the result to the Silver bucket.

    Returns the number of Silver records written.
    """
    from minio import Minio

    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

    client = Minio(endpoint, access_key=access, secret_key=secret, secure=False)

    # Read Bronze records. In Airflow, limit work to the active DAG run.
    # Manual runs without a run id keep the old whole-lake behavior.
    try:
        raw_jobs = _load_bronze_jobs(client, pipeline_run_id)
    except Exception as e:
        logger.error(f"Cannot access MinIO Bronze: {e}")
        return 0

    if not raw_jobs:
        logger.warning("No Bronze records found")
        return 0

    logger.info(f"Loaded {len(raw_jobs)} Bronze records")

    # ── Transform ──────────────────────────────────────────────
    processed_at = datetime.utcnow().isoformat()
    silver_jobs = []
    seen_urls = set()

    for job in raw_jobs:
        try:
            if job.get("pipeline_run_id") is None and pipeline_run_id:
                job["pipeline_run_id"] = pipeline_run_id

            transformed = transform_job(job, processed_at)
            transformed["is_low_quality"] = is_low_quality(transformed)

            url = transformed.get("url")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            silver_jobs.append(transformed)
        except Exception as e:
            logger.warning(f"Error transforming job: {e}")

    logger.info(f"Transformed {len(silver_jobs)} Silver records ({len(raw_jobs) - len(silver_jobs)} dropped)")

    # ── Write Silver ───────────────────────────────────────────
    if not silver_jobs:
        return 0

    content = json.dumps(silver_jobs, ensure_ascii=False, default=str).encode("utf-8")
    now = datetime.utcnow()
    safe_run_id = re.sub(r"[^A-Za-z0-9_.=-]+", "_", pipeline_run_id or "manual")
    object_path = f"jobs/{now.year}/{now.month:02d}/{safe_run_id}/silver_{now.strftime('%Y%m%d_%H%M%S')}.json"

    try:
        if not client.bucket_exists("silver"):
            client.make_bucket("silver")
        client.put_object(
            "silver", object_path,
            io.BytesIO(content), len(content),
            content_type="application/json",
            metadata={
                "pipeline_run_id": pipeline_run_id or "",
                "record_count": str(len(silver_jobs)),
                "processed_at": processed_at,
            }
        )
        logger.info(f"Silver written to s3://silver/{object_path}")
    except Exception as e:
        logger.error(f"Error writing Silver to MinIO: {e}")

    return len(silver_jobs)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    count = run_silver_transform(pipeline_run_id="manual_silver_test")
    print(f"Silver transform complete: {count} records")
