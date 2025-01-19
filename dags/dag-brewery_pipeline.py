from airflow import DAG
from airflow.decorators import dag
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta
import os
from spark_operator import SilverLayerOperator, WriteSilverLayerOperator
from brewery_operators import APIExtractorOperator, WriteBronzeLayerOperator

# Configuration Constants
API_URL = os.getenv('BREWERY_API_URL', "https://api.openbrewerydb.org/breweries")
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minio')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minio123')
BRONZE_BUCKET = 'bronze'
SILVER_BUCKET = 'silver'

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

    extract_breweries = APIExtractorOperator(
        task_id='extract_breweries',
        api_url=API_URL
    )

    store_bronze = WriteBronzeLayerOperator(
        task_id='store_bronze',
        bucket=BRONZE_BUCKET,
        minio_endpoint=MINIO_ENDPOINT,
        minio_access_key=MINIO_ACCESS_KEY,
        minio_secret_key=MINIO_SECRET_KEY
    )
    
    transform_silver = SilverLayerOperator(
        task_id='transform_to_silver',
        source_bucket=BRONZE_BUCKET,
        dest_bucket=SILVER_BUCKET,
        minio_endpoint=MINIO_ENDPOINT,
        minio_access_key=MINIO_ACCESS_KEY,
        minio_secret_key=MINIO_SECRET_KEY
    )

    write_silver = WriteSilverLayerOperator(
        task_id='write_silver',
        dest_bucket=SILVER_BUCKET,
        minio_endpoint=MINIO_ENDPOINT,
        minio_access_key=MINIO_ACCESS_KEY,
        minio_secret_key=MINIO_SECRET_KEY
    )


    check_api >> extract_breweries >> store_bronze >> transform_silver >> write_silver

dag = dag_breweries()