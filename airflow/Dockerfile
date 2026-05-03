FROM apache/airflow:2.8.1-python3.11

USER root

# Install system dependencies (for psycopg2, lxml, etc.)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy DAGs and custom modules
COPY dags/ /opt/airflow/dags/
