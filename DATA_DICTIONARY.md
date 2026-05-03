# Moroccan Job Market Analyzer — Data Dictionary

> **Version:** 1.0 | **Updated:** 2025-04-25 | **Layer:** All (Bronze → Silver → Gold)

---

## Bronze Layer — Raw JSON Schema

All fields are raw strings from scrapers. Nothing is normalized here.

| Field | Type | Source | Description |
|-------|------|--------|-------------|
| `uid` | string | computed | MD5 hash of the URL — stable unique identifier |
| `title` | string | scraper | Raw job title as displayed on the site |
| `company` | string | scraper | Company name as displayed |
| `city` | string | scraper | City/location field — not normalized |
| `sector` | string | scraper | Industry sector — not normalized |
| `contract_type` | string | scraper | Raw contract type field (CDI, CDD, etc.) |
| `salary` | string | scraper | Raw salary field (e.g., "15 000 - 20 000 MAD") |
| `date_posted` | string | scraper | Raw date as shown on site (various formats) |
| `deadline` | string | scraper | Application deadline, when available |
| `url` | string | scraper | Full canonical URL to the job posting |
| `description` | string | scraper | Full job description — may contain HTML |
| `source` | string | scraper | Source platform: `rekrute`, `emploi`, `indeed`, `bayt` |
| `scraped_at` | string | system | UTC timestamp when the record was scraped (ISO 8601) |
| `pipeline_run_id` | string | airflow | Airflow DAG run ID for lineage tracking |

---

## Silver Layer — Cleaned Parquet Schema

All fields are normalized, HTML-stripped, and validated.

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `uid` | string | no | Same MD5 hash from Bronze |
| `title` | string | no | Lowercased, HTML-stripped job title |
| `company` | string | yes | Whitespace-trimmed company name |
| `city` | string | yes | Canonical city name (see normalization map below) |
| `region` | string | yes | Moroccan region (derived from city) |
| `sector` | string | yes | Normalized industry sector |
| `contract_type` | string | no | One of: `CDI`, `CDD`, `Stage`, `Freelance`, `unknown` |
| `salary_min` | integer | yes | Minimum salary in MAD |
| `salary_max` | integer | yes | Maximum salary in MAD |
| `salary_currency` | string | yes | Currency code (default: `MAD`) |
| `date_posted` | date | yes | ISO 8601 date: `YYYY-MM-DD` |
| `deadline` | date | yes | ISO 8601 application deadline |
| `url` | string | no | Original URL (unique per record) |
| `description` | string | yes | HTML-stripped plain text description |
| `skills` | array[string] | yes | Extracted skills from description (e.g., `["Python", "Docker"]`) |
| `language` | string | yes | Detected language: `fr`, `ar`, or `en` |
| `is_remote` | boolean | yes | True if job mentions télétravail/remote |
| `is_low_quality` | boolean | no | True if record is missing critical fields |
| `source` | string | no | Source platform (unchanged from Bronze) |
| `scraped_at` | timestamp | no | Original scraping timestamp |
| `processed_at` | timestamp | no | When Silver transformation ran |
| `pipeline_run_id` | string | yes | Airflow run ID |

### City Normalization Rules
| Input (examples) | Canonical City |
|------------------|----------------|
| Casa, Grand Casablanca, Casablanca-Settat | **Casablanca** |
| Rabat-Salé, Salé | **Rabat** |
| Marrakesh, Marrakech-Safi | **Marrakech** |
| Fez, Fes, Fès-Meknès | **Fès** |
| Tangier, Tanger-Tétouan | **Tanger** |
| Télétravail, Remote, à distance | **Remote** |

### Skill Categories
| Category | Examples |
|----------|---------|
| `programming` | Python, Java, JavaScript, PHP, SQL |
| `cloud` | AWS, Azure, GCP |
| `data` | Apache Spark, Kafka, Airflow, Machine Learning |
| `framework` | React, Angular, Django, Spring Boot |
| `devops` | Docker, Kubernetes, Git, CI/CD |
| `database` | PostgreSQL, MongoDB, Redis, Elasticsearch |
| `visualization` | Power BI, Tableau |
| `soft` | Agile, Jira |

---

## Gold Layer — Aggregate Tables Schema

### `agg_top_skills_daily`
Pre-aggregated skill demand counts by day.

| Column | Type | Description |
|--------|------|-------------|
| `date` | date | Day of aggregation |
| `skill` | text | Skill name |
| `job_count` | integer | Number of job postings mentioning this skill |
| `city` | text | City filter (`all` = all cities) |
| `source` | text | Source filter (`all` = all sources) |
| `updated_at` | timestamp | When this aggregate was last computed |

### `agg_jobs_by_city`
Job posting counts per city per day.

| Column | Type | Description |
|--------|------|-------------|
| `city` | text | Canonical city name |
| `date` | date | Day of aggregation |
| `job_count` | integer | Number of postings in that city |
| `source` | text | Source filter |

### `agg_jobs_by_company`
Top hiring companies by posting volume.

| Column | Type | Description |
|--------|------|-------------|
| `company` | text | Company name |
| `date` | date | Day of aggregation |
| `job_count` | integer | Number of postings from that company |

### `agg_contract_distribution`
Breakdown of employment types.

| Column | Type | Description |
|--------|------|-------------|
| `contract_type` | text | `CDI`, `CDD`, `Stage`, `Freelance`, `unknown` |
| `date` | date | Day of aggregation |
| `job_count` | integer | Count |

### `agg_remote_jobs`
Remote vs on-site ratio.

| Column | Type | Description |
|--------|------|-------------|
| `date` | date | Day |
| `is_remote` | boolean | True = remote/télétravail |
| `job_count` | integer | Count |

### `agg_salary_stats`
Salary statistics by job title.

| Column | Type | Description |
|--------|------|-------------|
| `title` | text | Job title |
| `city` | text | City or `all` |
| `salary_min` | integer | Minimum salary seen (MAD) |
| `salary_max` | integer | Maximum salary seen (MAD) |
| `salary_avg` | numeric | Average of salary_min values (MAD) |
| `sample_size` | integer | Number of data points |

---

## Fact Table Schema (`dw.fact_jobs`)

One row per unique job posting (identified by URL).

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `job_id` | serial | PK | Auto-increment primary key |
| `title` | text | NOT NULL | Normalized job title |
| `company` | text | | Company name |
| `city` | text | | Canonical city |
| `sector` | text | | Industry sector |
| `contract_type` | text | CHECK | One of CDI/CDD/Stage/Freelance/unknown |
| `salary_min` | integer | | Min salary in MAD |
| `salary_max` | integer | | Max salary in MAD |
| `date_posted` | date | | When job was published |
| `source` | text | NOT NULL | Origin platform |
| `url` | text | UNIQUE | Canonical URL |
| `language` | text | | fr/ar/en |
| `is_remote` | boolean | | Whether remote work is mentioned |
| `is_low_quality` | boolean | | Quality flag |
| `description` | text | | Plain-text description |
| `pipeline_run_id` | text | | Airflow lineage ID |
| `scraped_at` | timestamp | NOT NULL | When scraped |
| `processed_at` | timestamp | | When Silver ran |
| `created_at` | timestamp | | Row insertion time |
