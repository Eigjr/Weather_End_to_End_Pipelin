FROM apache/airflow:3.1.0

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    build-essential \
    libpq-dev \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Mount the docker socket so DockerOperator can spin up the dbt container
RUN apt-get update && apt-get install -y --no-install-recommends \
    docker.io \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* || true

USER airflow

COPY requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.0/constraints-3.12.txt" \
    -r /opt/airflow/requirements.txt