import io
import pandas as pd
from psycopg2.extras import execute_values
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from python_script.location import loc

BUCKET = "weather-pipeline"

DDL = """
CREATE SCHEMA IF NOT EXISTS raw_data;

CREATE TABLE IF NOT EXISTS raw_data.weather_data (
    city TEXT NOT NULL,
    country TEXT,
    day DATE NOT NULL,
    temp_max DOUBLE PRECISION,
    temp_min DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    PRIMARY KEY (city, day)
);
"""

INSERT_SQL = """
INSERT INTO raw_data.weather_data
(city, country, day, temp_max, temp_min, wind_speed)
VALUES %s
ON CONFLICT (city, day) DO NOTHING;
"""

def read_parquet_from_minio(city):
    s3_hook = S3Hook(aws_conn_id="minio_conn")
    key = f"raw/{city.lower()}/weather.parquet"
    obj = s3_hook.get_key(key=key, bucket_name=BUCKET)
    data = obj.get()["Body"].read()
    df = pd.read_parquet(io.BytesIO(data))
    print("COLUMNS:", df.columns)  # debug
    return df

def prepare_rows(df, city, country):
    """
    Map dataframe columns to Postgres insert tuples
    """
    # Make sure your dataframe columns match exactly: 'date', 'temp_max', 'temp_min', 'wind_speed'
    rows = [
        (
            city,
            country,
            r["date"],
            r["temp_max"],
            r["temp_min"],
            r["wind_speed"]
        )
        for _, r in df.iterrows()
    ]
    return rows

def run():
    pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = pg_hook.get_conn()

    try:
        # create schema/table
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()

        for city, meta in loc.items():
            print(f"Loading {city} from MinIO...")
            df = read_parquet_from_minio(city)
            rows = prepare_rows(df, city, meta.get("country", ""))

            if not rows:
                print(f"No data found for {city}")
                continue

            with conn.cursor() as cur:
                execute_values(cur, INSERT_SQL, rows, page_size=1000)

            conn.commit()
            print(f"{city}: inserted {len(rows)} rows")

    finally:
        conn.close()
        print("All cities processed and connection closed.")