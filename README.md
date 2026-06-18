# Weather End-to-End Data Pipeline

This project is an end-to-end weather data pipeline designed to collect, store, transform historical weather data across multiple cities and countries. The pipeline captures key weather metrics, including country, city, date, maximum temperature, minimum temperature and wind speed.

The solution follows a modern data engineering architecture, moving data through the complete ELT lifecycle:

* **Extract:** Historical weather data is retrieved from the Open-Meteo API.
* **Load:** Raw data is stored in MinIO as Parquet files for scalable and cost-effective object storage.
* **Transform:** dbt is used to clean, standardize, and model the data into analytics-ready datasets.
* **Serve:** Transformed data is loaded into PostgreSQL, enabling efficient querying, reporting, and downstream analytics.

This project demonstrates a modern end-to-end data engineering architecture. It utilizes Apache Airflow for workflow orchestration, Docker for containerized deployment, MinIO for object storage and data lake management, PostgreSQL as the data warehouse, dbt for transformation and analytics engineering, Terraform for Infrastructure as Code (IaC), and GitHub Actions for continuous integration and deployment (CI/CD). The solution automates the ingestion, storage, transformation, and delivery of weather data into analytics-ready datasets.

The primary objective is to transform raw operational weather data into actionable intelligence that supports reporting, trend analysis, forecasting, and data-driven decision-making for both internal stakeholders and external partners.
---

## Table of contents

- [Architecture](#architecture)
- [Tech stack](#tech-stack)
- [Project structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Getting started](#getting-started)
- [Environment variables](#environment-variables)
- [Pipeline walkthrough](#pipeline-walkthrough)
- [dbt models](#dbt-models)
- [Email notifications](#email-notifications)
- [CI/CD](#cicd)
- [Roadmap](#roadmap)
- [License](#license)

---

## Architecture

```![Pipeline Architecture](image/Untitled Diagram.png)


``![Infrastructure](image/architectural diagram.png)

**DAG chain** (`dags/weather_data_pipeline`):

```
ingest_to_minio >> load_minio_to_postgres >> run_dbt >> send_notification
```

---

## Tech stack

| Layer | Tool | Notes |
|---|---|---|
| Orchestration | Apache Airflow 3.1.0 | Scheduler + Celery worker |
| Transformation | dbt-core + dbt-postgres | Runs via `DockerOperator` |
| Object storage | MinIO | S3-compatible; raw file staging |
| Database | PostgreSQL 15 | Load stage + dbt output |
| Containerisation | Docker + Docker Compose | All services containerised |
| CI/CD | GitHub Actions + Docker Hub | Push on merge to `main` |
| Language | Python 3.11 | DAG logic and ingestion |

---

## Project structure

```
Nova_weather pipeline/
│
├── airflow/
│   ├── airfloww/               # Airflow home directory (mounted into container)
│   ├── Dockerfile              # Airflow image — extends apache/airflow:3.1.0
│   └── requirements.txt        # Python dependencies for Airflow workers
│
├── config/
│   └── airflow.cfg             # Airflow configuration overrides
│
├── dags/
│   └── airflow/                # DAG files — weather_data_pipeline.py lives here
│
├── transformation/
│   ├── dbt_nova/               # dbt project root (dbt_project.yml, models/, etc.)
│   ├── Dockerfile              # dbt image — invoked by Airflow DockerOperator
│   ├── requirements.txt        # Python dependencies for the dbt container
│   ├── logs/                   # dbt run logs
│   └── target/                 # dbt compiled artefacts (git-ignored)
│
├── plugins/                    # Custom Airflow plugins (currently empty)
├── logs/                       # Airflow task logs
│   ├── dag_id=weather_data_pipeline/
│   └── dag_processor/
│
├── image/
│   ├── architectural diagram.png
│   └── Untitled Diagram.png
│
├── docker-compose.yaml         # Defines all services
├── README.md
│
# Note: datafile/ and me/ are local Python virtual environments.
# They are not part of the pipeline and should be added to .gitignore.
```

> **Important:** `datafile/` and `me/` are local `venv` directories (both contain `pyvenv.cfg`). Add them to `.gitignore` — they should never be committed to the repository.

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (or Docker Engine + Compose v2 on Linux)
- Git
- A Gmail account with an [App Password](https://support.google.com/accounts/answer/185833) enabled (for pipeline email alerts)

No local Python installation is required — everything runs inside Docker.

---

## Getting started

### 1. Clone the repository

```bash
git clone https://github.com/<your-username>/nova-weather-pipeline.git
cd nova-weather-pipeline
```

### 2. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` with your credentials. See [Environment variables](#environment-variables) for the full list.

### 3. Build images and start all services

```bash
docker compose up --build -d
```

This builds the `airflow/` and `transformation/` images separately and starts:

| Service | URL |
|---|---|
| Airflow webserver | http://localhost:8080 |
| MinIO console | http://localhost:9001 |
| PostgreSQL | `localhost:5432` |

### 4. Initialise Airflow on first run

```bash
docker compose exec airflow-webserver airflow db migrate
docker compose exec airflow-webserver airflow users create \
  --username admin \
  --password admin \
  --firstname Nova \
  --lastname Admin \
  --role Admin \
  --email admin@example.com
```

### 5. Set Airflow Variables for email credentials

The notification task reads credentials from Airflow Variables at runtime — never from code.

```bash
docker compose exec airflow-webserver airflow variables set NOTIFY_EMAIL_FROM   "your@gmail.com"
docker compose exec airflow-webserver airflow variables set NOTIFY_EMAIL_TO     "your@gmail.com"
docker compose exec airflow-webserver airflow variables set NOTIFY_APP_PASSWORD "your-16-char-app-password"
```

### 6. Trigger the pipeline

Log in to the Airflow UI at http://localhost:8080, enable the `weather_data_pipeline` DAG, and trigger a run manually — or let the schedule pick it up.

---

## Environment variables

Create a `.env` file in the project root (copy from `.env.example`). Docker Compose reads this file automatically.

```dotenv
# PostgreSQL
POSTGRES_USER=nova
POSTGRES_PASSWORD=changeme
POSTGRES_DB=nova_db
POSTGRES_PORT=5432

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=changeme
MINIO_PORT=9000
MINIO_CONSOLE_PORT=9001

# Airflow
AIRFLOW__CORE__FERNET_KEY=        # python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://nova:changeme@postgres/nova_db

# Email (set as Airflow Variables after init — do not rely on these at runtime)
NOTIFY_EMAIL_FROM=your@gmail.com
NOTIFY_EMAIL_TO=your@gmail.com
```

> **Never commit `.env` to version control.** It is listed in `.gitignore`.

---

## Pipeline walkthrough

### Task 1 — `ingest_to_minio` · Extract

A Python callable fetches the current weather payload from the API and writes it as a timestamped file (e.g. `weather_20250612_143000.json`) to the `raw-data/` bucket in MinIO. The raw file is preserved here untouched — MinIO is the single source of truth for ingested data.

### Task 2 — `load_minio_to_postgres` · Load

Reads the staged file from MinIO and inserts rows into a PostgreSQL staging table. No business logic is applied — data lands exactly as received from the source.

### Task 3 — `run_dbt` · Transform

An Airflow `DockerOperator` spins up the `transformation/` container and runs:

```bash
dbt run --project-dir /dbt_nova --profiles-dir /dbt_nova
```

dbt reads from the PostgreSQL staging table, applies the model logic in `transformation/dbt_nova/models/`, and writes clean, typed output tables back to a separate schema. Running dbt in its own container (separate from the Airflow image) keeps dependency trees isolated — the dbt container has no Airflow packages, and vice versa.

### Task 4 — `send_notification` · Alert

On successful completion of the dbt run, a Python callable sends an HTML summary email using `smtplib.SMTP_SSL` on port 465. The email reports the run date and pipeline status. Credentials are read from Airflow Variables — the sending logic contains no hardcoded secrets.

---

## dbt models

The dbt project lives in `transformation/dbt_nova/`.

```
dbt_nova/
├── dbt_project.yml
├── profiles.yml
└── models/
    ├── staging/
    │   └── stg_weather.sql        # Type casts and column renames on raw data
    └── marts/
        └── weather_daily.sql      # Aggregated daily weather metrics
```

To run dbt manually inside the transformation container:

```bash
# Run all models
docker compose run --rm dbt dbt run --project-dir /dbt_nova --profiles-dir /dbt_nova

# Run tests
docker compose run --rm dbt dbt test --project-dir /dbt_nova --profiles-dir /dbt_nova

# Generate and serve documentation
docker compose run --rm dbt dbt docs generate --project-dir /dbt_nova
docker compose run --rm dbt dbt docs serve --project-dir /dbt_nova --port 8001
```

---

## Email notifications

Notifications use Python's built-in `smtplib` over `SMTP_SSL` (port 465). No third-party email library is required.

**How it works:**

```python
import smtplib, ssl
from email.mime.text import MIMEText
from airflow.models import Variable

password = Variable.get("NOTIFY_APP_PASSWORD")
# ... build message, then:
with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ssl.create_default_context()) as server:
    server.login(sender, password)
    server.sendmail(sender, recipient, msg.as_string())
```

**Common issues:**

| Error | Fix |
|---|---|
| `SMTPAuthenticationError` | Confirm you are using a Gmail App Password, not your account password |
| `Connection refused` | Check port 465 is not blocked by your network or firewall |
| `Variable not found` | Run the `airflow variables set` commands from the [Getting started](#getting-started) section |

---

## CI/CD

GitHub Actions runs on every push and pull request targeting `main`.

```yaml
# .github/workflows/ci.yml
jobs:
  lint    # ruff (Python DAGs), sqlfluff (dbt SQL)
  test    # pytest — DAG import tests and unit tests
  build   # docker build for airflow/ and transformation/
  push    # docker push to Docker Hub (main branch only)
```

**Required GitHub Actions secrets:**

| Secret | Description |
|---|---|
| `DOCKERHUB_USERNAME` | Your Docker Hub username |
| `DOCKERHUB_TOKEN` | Docker Hub access token (not your password) |

**Published images:**

```
<dockerhub-username>/nova-airflow:latest
<dockerhub-username>/nova-dbt:latest
```

---

## Roadmap

- [ ] Add a `.env.example` file and document all required variables
- [ ] Add `datafile/` and `me/` to `.gitignore`
- [ ] Add Great Expectations data quality checks between load and transform
- [ ] Parameterise the weather API endpoint via Airflow Variables (swap any REST source)
- [ ] Add a live dashboard (Metabase or Apache Superset) over the `marts/` schema
- [ ] Provision cloud infrastructure with Terraform (AWS S3 + RDS or GCP equivalent)
- [ ] Extend dbt models with additional aggregation layers and dbt tests

---

## License

MIT — see [LICENSE](LICENSE) for details.