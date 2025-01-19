from airflow.models import BaseOperator
import requests
from minio import Minio
from io import BytesIO
import json
from datetime import datetime

class APIExtractorOperator(BaseOperator):
    def __init__(self, api_url, params=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api_url = api_url
        self.params = params or {}

    def execute(self, context):
        return self.extract_data()

    def extract_data(self):
        all_data = []
        page = 1
        per_page = 50
        params = self.params.copy()

        while True:
            params.update({'page': page, 'per_page': per_page})
            response = requests.get(self.api_url, params=params)
            
            if response.status_code != 200:
                self.log.error(f"Failed to fetch data: {response.status_code}")
                break
            
            data = response.json()
            if not data:
                break
            
            all_data.extend(data)
            self.log.info(f"Total items fetched: {len(all_data)}")

            if len(data) < per_page:
                break
            page += 1

        return all_data

class WriteBronzeLayerOperator(BaseOperator):
    def __init__(self, bucket, minio_endpoint, minio_access_key, minio_secret_key, 
                 data_key=None, filename_prefix='data', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.data_key = data_key
        self.filename_prefix = filename_prefix

    def execute(self, context):
        if self.data_key:
            data = context['task_instance'].xcom_pull(key=self.data_key)
        else:
            data = context['task_instance'].xcom_pull()
        return self.store_data(data)

    def store_data(self, data):
        client = Minio(
            self.minio_endpoint,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=False
        )
        
        if not client.bucket_exists(self.bucket):
            client.make_bucket(self.bucket)

        data_json = json.dumps(data)
        data_bytes = data_json.encode('utf-8')
        data_file = BytesIO(data_bytes)

        filename = f"{self.filename_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        client.put_object(
            self.bucket, 
            filename, 
            data_file, 
            length=len(data_bytes),
            content_type='application/json'
        )

        self.log.info(f"Data stored in MinIO bucket '{self.bucket}' as '{filename}'")
        return filename