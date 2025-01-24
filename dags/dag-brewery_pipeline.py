"""
This DAG orchestrates the brewery data pipeline with a Medallion Architecture:
1. Bronze layer: Store raw data from the OpenBreweryDB API.
2. Silver layer: Transform data into a columnar format (Parquet), partitioned by location.
3. Gold layer: Aggregate the number of breweries by type and location, and store results in PostgreSQL.
"""

from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta
import os
from brewery_operators import (
    APIExtractorOperator,
    WriteBronzeLayerOperator,
    ProcessSilverLayerOperator,
    ProcessGoldLayerOperator
)
from data_quality_operator import DataQualityOperator

# Environment-based configurations for API, MinIO, and PostgreSQL.
API_URL = os.getenv('BREWERY_API_URL', "https://api.openbrewerydb.org/breweries")
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minio')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minio123')

BRONZE_BUCKET = 'bronze'
SILVER_BUCKET = 'silver'
GOLD_BUCKET = 'gold'

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres-analytics')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', 5432)
POSTGRES_DB = os.getenv('POSTGRES_DB', 'brewery_analytics')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'analytics')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'analytics123')

default_args = {
    "owner": "Henrique",
    "start_date": datetime(2024, 10, 1),
    "retry_delay": timedelta(minutes=5),
    "retries": 3,
}

# Define the DAG, which runs on a daily schedule and does not backfill older dates.
with DAG(
    dag_id="brewery_pipeline",
    default_args=default_args,
    description="DAG to process brewery data through Bronze, Silver, and Gold layers",
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Task 1: Verify the Brewery API is reachable (receives HTTP 200).
    check_api = HttpSensor(
        task_id="check_api",
        http_conn_id="brewery_api",  # Must be configured in Airflow Connections
        endpoint="/breweries",
        method="GET",
        response_check=lambda response: response.status_code == 200,
        timeout=30,
        poke_interval=10,
        mode="reschedule",
    )

    # Task 2: Extract brewery data from the API as raw JSON.
    extract_breweries = APIExtractorOperator(
        task_id="extract_breweries",
        api_url=API_URL
    )

    # Task 3: Persist the raw data (in JSON) into the Bronze bucket on MinIO.
    store_bronze = WriteBronzeLayerOperator(
        task_id="store_bronze",
        bucket=BRONZE_BUCKET,
        minio_endpoint=MINIO_ENDPOINT,
        minio_access_key=MINIO_ACCESS_KEY,
        minio_secret_key=MINIO_SECRET_KEY
    )

    # Task 4: Read the raw data from Bronze, transform it into Parquet, partition by location,
    # and store it in the Silver bucket.
    process_silver = ProcessSilverLayerOperator(
        task_id="process_silver",
        source_bucket=BRONZE_BUCKET,
        destination_bucket=SILVER_BUCKET,
        minio_endpoint=MINIO_ENDPOINT,
        minio_access_key=MINIO_ACCESS_KEY,
        minio_secret_key=MINIO_SECRET_KEY
    )
    
    # Task 5: 
    
    data_quality_check = DataQualityOperator(
    task_id="data_quality_check",
    source_bucket=SILVER_BUCKET,
    minio_endpoint=MINIO_ENDPOINT,
    minio_access_key=MINIO_ACCESS_KEY,
    minio_secret_key=MINIO_SECRET_KEY,
    min_record_count=100,
    critical_columns=["id", "brewery_name", "brewery_type"],
)

    # Task 6: Aggregate the Silver data by brewery type and location, store in the Gold bucket,
    # and write the aggregated results to PostgreSQL.
    process_gold = ProcessGoldLayerOperator(
        task_id="process_gold",
        source_bucket=SILVER_BUCKET,
        destination_bucket=GOLD_BUCKET,
        minio_endpoint=MINIO_ENDPOINT,
        minio_access_key=MINIO_ACCESS_KEY,
        minio_secret_key=MINIO_SECRET_KEY,
        pg_host=POSTGRES_HOST,
        pg_port=POSTGRES_PORT,
        pg_db=POSTGRES_DB,
        pg_user=POSTGRES_USER,
        pg_password=POSTGRES_PASSWORD
    )

    # Define the pipeline order to ensure each layer is processed sequentially.
    check_api >> extract_breweries >> store_bronze >> process_silver >> data_quality_check >> process_gold