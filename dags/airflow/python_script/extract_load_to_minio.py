import io
import logging
import requests
import pandas as pd
from datetime import date, timedelta
from python_script.location import loc
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Configure logger
logger = logging.getLogger(__name__)

DAILY_FIELDS = "temperature_2m_max,temperature_2m_min,wind_speed_10m_max"


def extract_and_upload_weather_parquet(
    base_url: str,
    city: str,
    country: str,
    lat: float,
    lon: float,
    bucket: str,
    object_name: str,
    conn_id: str,
    days_back: int = 1000,
):
    """
    Fetch weather data from Open-Meteo, convert to Parquet in memory, and upload to MinIO.
    """
    try:
        logger.info("Starting weather extraction for %s, %s", city, country)

        end_date = date.today()
        start_date = end_date - timedelta(days=days_back)

        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "daily": DAILY_FIELDS,
            "timezone": "UTC",
        }

        logger.info("Calling Open-Meteo API")
        resp = requests.get(base_url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()["daily"]
        logger.info("API response received successfully")

        # Convert API data to a Pandas DataFrame
        df = pd.DataFrame({
            "city": city,
            "country": country,
            "date": data["time"],
            "temp_max": data["temperature_2m_max"],
            "temp_min": data["temperature_2m_min"],
            "wind_speed": data["wind_speed_10m_max"],
        })

        logger.info("DataFrame created with %d rows", len(df))

        # Write Parquet to in-memory buffer
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        # Upload Parquet directly to MinIO
        s3_hook = S3Hook(aws_conn_id=conn_id)
        s3_hook.load_bytes(
            bytes_data=buffer.getvalue(),
            key=object_name,
            bucket_name=bucket,
            replace=True,
        )

        logger.info("Uploaded Parquet data to s3://%s/%s", bucket, object_name)

        buffer.close()
        logger.info("Memory buffer cleared")

    except requests.exceptions.RequestException as api_error:
        logger.error("API request failed for %s: %s", city, api_error)
        raise

    except Exception as e:
        logger.error("Weather pipeline failed for %s: %s", city, e)
        raise