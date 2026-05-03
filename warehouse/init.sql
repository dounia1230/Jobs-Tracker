-- ============================================================
-- Moroccan Job Market Analyzer — PostgreSQL Data Warehouse
-- Schema Initialization Script
-- ============================================================
-- This script runs automatically when the postgres container
-- starts for the first time.
-- ============================================================

-- ─── Extensions ──────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- For fuzzy text search

-- ─── Schema ──────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS dw;
SET search_path TO dw, public;

-- ─── FACT TABLE: One row per unique job posting ──────────────
CREATE TABLE IF NOT EXISTS dw.fact_jobs (
    job_id          SERIAL PRIMARY KEY,
    title           TEXT NOT NULL,
    company         TEXT,
    city            TEXT,
    region          TEXT,
    sector          TEXT,
    contract_type   TEXT CHECK (contract_type IN ('CDI','CDD','Stage','Freelance','unknown')),
    salary_min      INTEGER,
    salary_max      INTEGER,
    salary_currency TEXT DEFAULT 'MAD',
    experience_years_min INTEGER,
    experience_years_max INTEGER,
    education_level TEXT,
    date_posted     DATE,
    deadline        DATE,
    source          TEXT NOT NULL,           -- rekrute | emploi | indeed | bayt
    url             TEXT UNIQUE NOT NULL,
    language        TEXT DEFAULT 'fr',       -- fr | ar | en
    is_remote       BOOLEAN DEFAULT FALSE,
    is_low_quality  BOOLEAN DEFAULT FALSE,
    description     TEXT,
    pipeline_run_id TEXT,
    scraped_at      TIMESTAMP NOT NULL,
    processed_at    TIMESTAMP DEFAULT NOW(),
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_fact_jobs_city     ON dw.fact_jobs(city);
CREATE INDEX IF NOT EXISTS idx_fact_jobs_source   ON dw.fact_jobs(source);
CREATE INDEX IF NOT EXISTS idx_fact_jobs_date     ON dw.fact_jobs(date_posted);
CREATE INDEX IF NOT EXISTS idx_fact_jobs_company  ON dw.fact_jobs(company);
CREATE INDEX IF NOT EXISTS idx_fact_jobs_sector   ON dw.fact_jobs(sector);
CREATE INDEX IF NOT EXISTS idx_fact_jobs_contract ON dw.fact_jobs(contract_type);
CREATE INDEX IF NOT EXISTS idx_fact_jobs_scraped  ON dw.fact_jobs(scraped_at);
-- Full text search on title
CREATE INDEX IF NOT EXISTS idx_fact_jobs_title_trgm ON dw.fact_jobs USING gin(title gin_trgm_ops);

-- ─── DIMENSION: Skills per job posting ───────────────────────
CREATE TABLE IF NOT EXISTS dw.dim_job_skills (
    id      SERIAL PRIMARY KEY,
    job_id  INTEGER REFERENCES dw.fact_jobs(job_id) ON DELETE CASCADE,
    skill   TEXT NOT NULL,
    UNIQUE (job_id, skill)
);

CREATE INDEX IF NOT EXISTS idx_dim_skills_job   ON dw.dim_job_skills(job_id);
CREATE INDEX IF NOT EXISTS idx_dim_skills_skill ON dw.dim_job_skills(skill);

-- ─── AGGREGATE: Top skills per day ───────────────────────────
CREATE TABLE IF NOT EXISTS dw.agg_top_skills_daily (
    date        DATE NOT NULL,
    skill       TEXT NOT NULL,
    job_count   INTEGER NOT NULL DEFAULT 0,
    city        TEXT DEFAULT 'all',
    source      TEXT DEFAULT 'all',
    updated_at  TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (date, skill, city, source)
);

CREATE INDEX IF NOT EXISTS idx_agg_skills_date  ON dw.agg_top_skills_daily(date);
CREATE INDEX IF NOT EXISTS idx_agg_skills_skill ON dw.agg_top_skills_daily(skill);

-- ─── AGGREGATE: Jobs per city ────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.agg_jobs_by_city (
    city         TEXT NOT NULL,
    date         DATE NOT NULL,
    job_count    INTEGER NOT NULL DEFAULT 0,
    source       TEXT DEFAULT 'all',
    updated_at   TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (city, date, source)
);

-- ─── AGGREGATE: Jobs per company ─────────────────────────────
CREATE TABLE IF NOT EXISTS dw.agg_jobs_by_company (
    company      TEXT NOT NULL,
    date         DATE NOT NULL,
    job_count    INTEGER NOT NULL DEFAULT 0,
    updated_at   TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (company, date)
);

-- ─── AGGREGATE: Jobs per sector ──────────────────────────────
CREATE TABLE IF NOT EXISTS dw.agg_jobs_by_sector (
    sector       TEXT NOT NULL,
    date         DATE NOT NULL,
    job_count    INTEGER NOT NULL DEFAULT 0,
    updated_at   TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (sector, date)
);

-- ─── AGGREGATE: Salary statistics ────────────────────────────
CREATE TABLE IF NOT EXISTS dw.agg_salary_stats (
    title        TEXT NOT NULL,
    city         TEXT NOT NULL DEFAULT 'all',
    sector       TEXT DEFAULT 'all',
    salary_min   INTEGER,
    salary_max   INTEGER,
    salary_avg   NUMERIC(10,2),
    sample_size  INTEGER,
    date         DATE NOT NULL,
    updated_at   TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (title, city, date)
);

-- ─── AGGREGATE: Contract type distribution ───────────────────
CREATE TABLE IF NOT EXISTS dw.agg_contract_distribution (
    contract_type TEXT NOT NULL,
    date          DATE NOT NULL,
    job_count     INTEGER NOT NULL DEFAULT 0,
    source        TEXT DEFAULT 'all',
    updated_at    TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (contract_type, date, source)
);

-- ─── AGGREGATE: Posting velocity (hourly) ────────────────────
CREATE TABLE IF NOT EXISTS dw.agg_posting_velocity (
    bucket_hour  TIMESTAMP NOT NULL,   -- Truncated to hour
    source       TEXT NOT NULL DEFAULT 'all',
    job_count    INTEGER NOT NULL DEFAULT 0,
    updated_at   TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (bucket_hour, source)
);

-- ─── AGGREGATE: Remote jobs ───────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.agg_remote_jobs (
    date         DATE NOT NULL,
    is_remote    BOOLEAN NOT NULL,
    job_count    INTEGER NOT NULL DEFAULT 0,
    source       TEXT DEFAULT 'all',
    updated_at   TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (date, is_remote, source)
);

-- ─── METADATA: Pipeline run log ──────────────────────────────
CREATE TABLE IF NOT EXISTS dw.pipeline_runs (
    run_id       TEXT PRIMARY KEY,
    dag_id       TEXT,
    started_at   TIMESTAMP,
    finished_at  TIMESTAMP,
    status       TEXT CHECK (status IN ('running','success','failed','partial')),
    jobs_scraped INTEGER DEFAULT 0,
    jobs_silver  INTEGER DEFAULT 0,
    jobs_gold    INTEGER DEFAULT 0,
    error_msg    TEXT
);

-- ─── METADATA: Data quality log ──────────────────────────────
CREATE TABLE IF NOT EXISTS dw.quality_reports (
    report_id    SERIAL PRIMARY KEY,
    run_id       TEXT REFERENCES dw.pipeline_runs(run_id),
    layer        TEXT CHECK (layer IN ('bronze','silver','gold')),
    check_name   TEXT,
    passed       BOOLEAN,
    details      TEXT,
    created_at   TIMESTAMP DEFAULT NOW()
);

-- ─── VIEWS: Convenient query shortcuts ───────────────────────

-- View: Latest top 20 skills
CREATE OR REPLACE VIEW dw.v_top_skills AS
SELECT
    s.skill,
    SUM(s.job_count) AS total_jobs,
    MAX(s.date) AS last_seen
FROM dw.agg_top_skills_daily s
WHERE s.city = 'all' AND s.source = 'all'
  AND s.date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY s.skill
ORDER BY total_jobs DESC
LIMIT 20;

-- View: Jobs posted in last 7 days by source
CREATE OR REPLACE VIEW dw.v_recent_jobs_by_source AS
SELECT
    source,
    COUNT(*) AS job_count,
    MIN(date_posted) AS oldest,
    MAX(date_posted) AS newest
FROM dw.fact_jobs
WHERE date_posted >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY source
ORDER BY job_count DESC;

-- View: Remote vs on-site ratio
CREATE OR REPLACE VIEW dw.v_remote_ratio AS
SELECT
    CASE WHEN is_remote THEN 'Remote/Télétravail' ELSE 'On-site' END AS work_mode,
    COUNT(*) AS job_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM dw.fact_jobs
WHERE date_posted >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY is_remote;

-- Dashboard: Tendances d'actualité / posting trends by day and source
CREATE OR REPLACE VIEW dw.v_dashboard_trends AS
SELECT
    COALESCE(date_posted, scraped_at::date) AS date,
    source,
    COUNT(*) AS total_offres,
    COUNT(*) FILTER (WHERE is_remote) AS offres_remote,
    COUNT(*) FILTER (WHERE is_low_quality) AS offres_low_quality
FROM dw.fact_jobs
GROUP BY COALESCE(date_posted, scraped_at::date), source
ORDER BY date DESC, source;

-- Dashboard: Nombre d'articles/offres par source
CREATE OR REPLACE VIEW dw.v_dashboard_source_counts AS
SELECT
    source,
    COUNT(*) AS total_offres,
    COUNT(*) FILTER (WHERE date_posted >= CURRENT_DATE - INTERVAL '7 days') AS total_7j,
    COUNT(*) FILTER (WHERE date_posted >= CURRENT_DATE - INTERVAL '30 days') AS total_30j,
    MIN(scraped_at) AS first_seen,
    MAX(scraped_at) AS last_seen
FROM dw.fact_jobs
GROUP BY source
ORDER BY total_offres DESC;

-- Dashboard: Mots clés les plus fréquents (skills extracted from content)
CREATE OR REPLACE VIEW dw.v_dashboard_top_keywords AS
SELECT
    s.skill AS keyword,
    COUNT(*) AS total_mentions,
    COUNT(DISTINCT s.job_id) AS total_offres,
    MAX(j.scraped_at) AS last_seen
FROM dw.dim_job_skills s
JOIN dw.fact_jobs j ON j.job_id = s.job_id
GROUP BY s.skill
ORDER BY total_mentions DESC, keyword;

-- Dashboard: Qualité des données by run/layer
CREATE OR REPLACE VIEW dw.v_dashboard_quality_summary AS
SELECT
    run_id,
    layer,
    COUNT(*) AS total_checks,
    COUNT(*) FILTER (WHERE passed) AS passed_checks,
    COUNT(*) FILTER (WHERE NOT passed) AS failed_checks,
    ROUND(COUNT(*) FILTER (WHERE passed) * 100.0 / NULLIF(COUNT(*), 0), 2) AS pass_rate,
    MAX(created_at) AS last_checked_at
FROM dw.quality_reports
GROUP BY run_id, layer
ORDER BY last_checked_at DESC, run_id, layer;

-- Governance: latest pipeline runs with audit counts
CREATE OR REPLACE VIEW dw.v_governance_pipeline_runs AS
SELECT
    run_id,
    dag_id,
    status,
    started_at,
    finished_at,
    EXTRACT(EPOCH FROM (finished_at - started_at))::INTEGER AS duration_seconds,
    jobs_scraped,
    jobs_silver,
    jobs_gold,
    error_msg
FROM dw.pipeline_runs
ORDER BY started_at DESC;

-- ─── Seed: Common Moroccan city list (for normalization ref) ─
CREATE TABLE IF NOT EXISTS dw.ref_cities (
    canonical_name TEXT PRIMARY KEY,
    aliases        TEXT[]
);

INSERT INTO dw.ref_cities (canonical_name, aliases) VALUES
    ('Casablanca', ARRAY['Casa','Grand Casablanca','Casablanca-Settat','Dar El Beida']),
    ('Rabat', ARRAY['Rabat-Salé','Rabat-Salé-Kénitra','Salé']),
    ('Marrakech', ARRAY['Marrakesh','Marrakech-Safi']),
    ('Fès', ARRAY['Fez','Fes','Fès-Meknès']),
    ('Tanger', ARRAY['Tangier','Tanger-Tétouan','Tanger-Tétouan-Al Hoceïma']),
    ('Agadir', ARRAY['Souss-Massa']),
    ('Oujda', ARRAY['Oriental']),
    ('Meknès', ARRAY['Meknes']),
    ('Kénitra', ARRAY['Kenitra']),
    ('Tétouan', ARRAY['Tetouan'])
ON CONFLICT (canonical_name) DO NOTHING;

-- ─── Seed: Common Moroccan tech skills list ──────────────────
CREATE TABLE IF NOT EXISTS dw.ref_skills (
    skill          TEXT PRIMARY KEY,
    category       TEXT,   -- programming | cloud | data | framework | soft
    aliases        TEXT[]
);

INSERT INTO dw.ref_skills (skill, category, aliases) VALUES
    ('Python', 'programming', ARRAY['python3','py']),
    ('SQL', 'programming', ARRAY['mysql','postgresql','oracle sql','pl/sql']),
    ('Java', 'programming', ARRAY['java8','java11','java17','jvm']),
    ('JavaScript', 'programming', ARRAY['js','nodejs','node.js','typescript','ts']),
    ('PHP', 'programming', ARRAY['php7','php8','laravel','symfony']),
    ('C#', 'programming', ARRAY['csharp','dotnet','.net','asp.net']),
    ('C++', 'programming', ARRAY['cpp','c plus plus']),
    ('R', 'programming', ARRAY['r programming','rstats']),
    ('Scala', 'programming', ARRAY['scala spark']),
    ('React', 'framework', ARRAY['reactjs','react.js']),
    ('Angular', 'framework', ARRAY['angularjs','angular2']),
    ('Vue.js', 'framework', ARRAY['vuejs','vue']),
    ('Django', 'framework', ARRAY['django rest']),
    ('Flask', 'framework', ARRAY['flask api']),
    ('Spring Boot', 'framework', ARRAY['spring','springboot']),
    ('Docker', 'devops', ARRAY['dockerfile','docker compose','docker-compose']),
    ('Kubernetes', 'devops', ARRAY['k8s','kubectl']),
    ('Git', 'devops', ARRAY['github','gitlab','bitbucket','version control']),
    ('AWS', 'cloud', ARRAY['amazon web services','ec2','s3','lambda']),
    ('Azure', 'cloud', ARRAY['microsoft azure','azure devops']),
    ('GCP', 'cloud', ARRAY['google cloud','bigquery']),
    ('Apache Spark', 'data', ARRAY['pyspark','spark','spark sql']),
    ('Hadoop', 'data', ARRAY['hdfs','mapreduce','hive']),
    ('Kafka', 'data', ARRAY['apache kafka','confluent']),
    ('Airflow', 'data', ARRAY['apache airflow']),
    ('Power BI', 'visualization', ARRAY['powerbi','power bi desktop']),
    ('Tableau', 'visualization', ARRAY[]),
    ('Machine Learning', 'data', ARRAY['ml','supervised learning','classification','regression']),
    ('Deep Learning', 'data', ARRAY['neural network','cnn','lstm','transformer']),
    ('TensorFlow', 'data', ARRAY['tensorflow2','keras']),
    ('PyTorch', 'data', ARRAY['torch']),
    ('Linux', 'devops', ARRAY['ubuntu','centos','bash','shell scripting']),
    ('Agile', 'soft', ARRAY['scrum','kanban','sprint']),
    ('Jira', 'soft', ARRAY[]),
    ('MongoDB', 'database', ARRAY['mongo']),
    ('Redis', 'database', ARRAY[]),
    ('Elasticsearch', 'database', ARRAY['elastic','elk stack']),
    ('PostgreSQL', 'database', ARRAY['postgres','psql']),
    ('MySQL', 'database', ARRAY[]),
    ('Oracle', 'database', ARRAY['oracle db','plsql']),
    ('REST API', 'architecture', ARRAY['restful','rest','api','web service']),
    ('Microservices', 'architecture', ARRAY['microservice','soa']),
    ('CI/CD', 'devops', ARRAY['jenkins','gitlab ci','github actions','continuous integration'])
ON CONFLICT (skill) DO NOTHING;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dw TO jobadmin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA dw TO jobadmin;

\echo '✅ Job Market DW schema initialized successfully'
