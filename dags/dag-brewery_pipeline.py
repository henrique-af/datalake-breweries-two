from airflow import DAG
from airflow.decorators import dag
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta
import os
from brewery_operators import ExtractBreweriesOperator, StoreDataMinIOOperator

# Configuration Constants
API_URL = os.getenv('BREWERY_API_URL', "https://api.openbrewerydb.org/breweries")
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minio')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minio123')
BRONZE_BUCKET = 'bronze'

default_args = {
    'owner': 'Henrique',
    'start_date': datetime(2024, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    default_args=default_args,
    description='DAG to consume data from Breweries API and store data in MinIO',
    schedule_interval='@daily',
    catchup=False,
)
def dag_breweries():
    check_api = HttpSensor(
        task_id='check_api',
        http_conn_id='brewery_api',
        endpoint='/breweries',
        method='GET',
        response_check=lambda response: response.status_code == 200,
        timeout=30,
        poke_interval=10,
        mode='reschedule',
    )

    extract_breweries = ExtractBreweriesOperator(
        task_id='extract_breweries',
        api_url=API_URL
    )

    store_breweries = StoreDataMinIOOperator(
        task_id='store_breweries',
        bucket=BRONZE_BUCKET,
        minio_endpoint=MINIO_ENDPOINT,
        minio_access_key=MINIO_ACCESS_KEY,
        minio_secret_key=MINIO_SECRET_KEY
    )

    check_api >> extract_breweries >> store_breweries

dag = dag_breweries()