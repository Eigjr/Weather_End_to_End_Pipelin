from airflow.sdk import dag, task
from datetime import timedelta
from pendulum import datetime
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from python_script.extract_load_to_minio import extract_and_upload_weather_parquet
from python_script.load_to_postgres import run
from python_script.location import loc

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
BUCKET = "weather-pipeline"
CONN_ID_MINIO = "minio_conn"
CONN_ID_POSTGRES = "postgres_conn"


@dag(
    dag_id="weather_data_pipeline",
    start_date=datetime(2026, 3, 10),
    schedule="@daily",
    catchup=False,
    description="Full ELT: API → MinIO → Postgres → dbt → Notification",
    tags=["weather", "dbt", "minio", "postgres"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
)
def weather_etl_pipeline():

    @task
    def ingest_to_minio():
        """EXTRACT: Fetch API data and LOAD: Upload to MinIO"""
        results = []
        for city_name, info in loc.items():
            object_key = f"raw/{city_name.lower()}/weather.parquet"
            extract_and_upload_weather_parquet(
                base_url=BASE_URL,
                city=city_name,
                country=info["country"],
                lat=info["lat"],
                lon=info["lon"],
                bucket=BUCKET,
                object_name=object_key,
                conn_id=CONN_ID_MINIO,
                days_back=1000,
            )
            results.append(city_name)
        return results

    @task
    def load_minio_to_postgres(cities_processed: list):
        """LOAD: Move data from MinIO to Postgres"""
        print(f"Moving data for {cities_processed} from MinIO to Postgres...")
        run()
        return "Data loaded to Postgres"

    run_dbt = DockerOperator(
        task_id="run_dbt_transformations",
        image="nova-dbt:latest",
        command="dbt run --project-dir /transformation/dbt_nova --profiles-dir /transformation/.dbt",
        mounts=[
            Mount(
                source="/home/ema-i/Desktop/test_work/transformation",
                target="/transformation",
                type="bind",
            )
        ],
        network_mode="test_work_default",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        auto_remove="success",
    )

    @task
    def send_notification(cities_processed: list, dbt_status: str, **context):
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from airflow.models import Variable

        sender = "emaigbo1@gmail.com"
        receiver = "emmanuelenya7@gmail.com"

        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"Weather Pipeline Success — {context.get('ds')}"
        msg["From"] = sender
        msg["To"] = receiver
        msg.attach(MIMEText(f"""
            <h3>Weather ELT Pipeline Completed</h3>
            <p><b>Status:</b> {dbt_status}</p>
            <p><b>Cities:</b> {", ".join(cities_processed)}</p>
            <p><b>Date:</b> {context.get('ds')}</p>
        """, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, Variable.get("gmail_app_password"))
            server.sendmail(sender, receiver, msg.as_string())


    # --- DAG flow ---
    cities = ingest_to_minio()
    loaded = load_minio_to_postgres(cities)
    loaded >> run_dbt >> send_notification(cities, "DBT Completed")


weather_etl_pipeline()