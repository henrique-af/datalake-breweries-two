from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
from minio import Minio, S3Error
from io import BytesIO
import json
from datetime import datetime
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lower, trim, count

class APIExtractorOperator(BaseOperator):
    """
    Extracts data from the Brewery API (OpenBreweryDB) in a paginated form.
    """
    @apply_defaults
    def __init__(self, api_url, params=None, *args, **kwargs):
        """
        :param api_url: The URL of the API endpoint to fetch breweries from.
        """
        super().__init__(*args, **kwargs)
        self.api_url = api_url
        self.params = params or {}

    def execute(self, context):
        """
        Executes the API call repeatedly using page and per_page parameters 
        until no more data is returned. Logs progress at each iteration.
        """
        self.log.info(f"Extracting data from API: {self.api_url}")
        all_data = []
        page = 1
        per_page = 50
        params = self.params.copy()

        while True:
            # Merge pagination details into current params
            params.update({'page': page, 'per_page': per_page})
            response = requests.get(self.api_url, params=params)

            if response.status_code != 200:
                self.log.error(f"Failed to fetch data: {response.status_code}")
                break
            
            data = response.json()
            if not data:
                # If no data is returned, end the loop
                break
            
            all_data.extend(data)
            self.log.info(f"Total items fetched: {len(all_data)}")

            # If less than 'per_page' items are returned, assume no more pages
            if len(data) < per_page:
                break
            page += 1

        return all_data


class WriteBronzeLayerOperator(BaseOperator):
    """
    Writes raw data in JSON format to a Bronze layer bucket on MinIO.
    """
    @apply_defaults
    def __init__(
        self,
        bucket,
        minio_endpoint,
        minio_access_key,
        minio_secret_key,
        data_key=None,
        filename_prefix='data',
        *args,
        **kwargs
    ):
        """
        :param bucket: The name of the MinIO bucket for storing raw data.
        :param minio_endpoint: Host and port of the MinIO service (e.g., "minio:9000").
        :param minio_access_key: Access key for MinIO authentication.
        :param minio_secret_key: Secret key for MinIO authentication.
        :param data_key: (Optional) XCom key to pull data from; if None, pulls from default.
        :param filename_prefix: (Optional) Prefix for naming the stored file.
        """
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.data_key = data_key
        self.filename_prefix = filename_prefix

    def execute(self, context):
        """
        Retrieves data from XCom, converts it to JSON, and stores it in the specified
        MinIO bucket. Creates the bucket if it does not exist.
        """
        self.log.info(f"Writing data to Bronze layer: {self.bucket}")

        if self.data_key:
            data = context['ti'].xcom_pull(key=self.data_key)
        else:
            data = context['ti'].xcom_pull()

        # Initialize the MinIO client
        client = Minio(
            self.minio_endpoint,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=False
        )
        
        # Ensure the bucket exists
        if not client.bucket_exists(self.bucket):
            client.make_bucket(self.bucket)

        # Convert the Python data to JSON and store in MinIO
        data_json = json.dumps(data)
        data_bytes = data_json.encode('utf-8')
        data_file = BytesIO(data_bytes)

        # Generate a timestamped filename
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


class ProcessSilverLayerOperator(BaseOperator):
    """
    Uses Spark to transform raw JSON data in the Bronze layer into a Parquet-based Silver layer.
    - Reads JSON data from Bronze
    - Applies cleaning / transformation
    - Writes partitioned Parquet data to Silver
    """
    @apply_defaults
    def __init__(
        self,
        source_bucket,
        destination_bucket,
        minio_endpoint,
        minio_access_key,
        minio_secret_key,
        *args,
        **kwargs
    ):
        """
        :param source_bucket: Name of the bucket containing raw JSON data (Bronze layer).
        :param destination_bucket: Name of the bucket to store the transformed data (Silver layer).
        :param minio_endpoint: Host and port of the MinIO service.
        :param minio_access_key: Access key for MinIO authentication.
        :param minio_secret_key: Secret key for MinIO authentication.
        """
        super().__init__(*args, **kwargs)
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key

    def execute(self, context):
        """
        Main routine:
         1) Initializes Spark
         2) Reads JSON from the Bronze bucket
         3) Transforms the data into a standardized schema
         4) Writes the result in Parquet, partitioned by 'state'
        """
        self.log.info("Processing data from Bronze to Silver layer")
        spark = self._get_spark_session()
        
        try:
            # Read data from Bronze as JSON
            bronze_data = spark.read.json(f"s3a://{self.source_bucket}/")
            
            # Transform the JSON data into a curated Silver format
            silver_data = self._transform_to_silver(bronze_data)
            
            # Write data to the Silver bucket as Parquet, partitioned by state
            silver_path = f"s3a://{self.destination_bucket}/silver_data"
            silver_data.write.partitionBy("state").mode("overwrite").parquet(silver_path)
            
            self.log.info(f"Silver data written to {silver_path}")
        finally:
            spark.stop()

    def _get_spark_session(self):
        """
        Builds/returns a SparkSession configured to read/write from an S3-compatible 
        storage (MinIO) using Hadoop AWS library.
        """
        return SparkSession.builder \
            .appName("ProcessSilverLayer") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{self.minio_endpoint}") \
            .config("spark.hadoop.fs.s3a.access.key", self.minio_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", self.minio_secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()

    @staticmethod
    def _transform_to_silver(df):
        """
        Defines how to transform the raw DataFrame:
         - Renames 'name' to 'brewery_name'
         - Converts 'brewery_type' to lowercase
         - Trims 'city', merges address into 'full_address'
         - Casts longitude/latitude to double
        """
        return df.select(
            col("id"),
            trim(col("name")).alias("brewery_name"),
            lower(col("brewery_type")).alias("brewery_type"),
            concat_ws(", ", col("address_1"), col("address_2"), col("address_3")).alias("full_address"),
            trim(col("city")).alias("city"),
            col("state_province").alias("state"),
            col("country"),
            col("longitude").cast("double"),
            col("latitude").cast("double")
        )


class ProcessGoldLayerOperator(BaseOperator):
    """
    Reads the Silver data with Spark, aggregates info into the Gold layer,
    and stores the aggregated data in both MinIO and a PostgreSQL database.
    """
    @apply_defaults
    def __init__(
        self,
        source_bucket,
        destination_bucket,
        minio_endpoint,
        minio_access_key,
        minio_secret_key,
        pg_host,
        pg_port,
        pg_db,
        pg_user,
        pg_password,
        *args,
        **kwargs
    ):
        """
        :param source_bucket: Bucket to read the Silver Parquet data.
        :param destination_bucket: Bucket to write aggregated Gold data.
        :param pg_host: Hostname for PostgreSQL server.
        :param pg_port: Port for PostgreSQL.
        :param pg_db: Database name in PostgreSQL.
        :param pg_user: PostgreSQL user.
        :param pg_password: PostgreSQL password.
        """
        super().__init__(*args, **kwargs)
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.pg_host = pg_host
        self.pg_port = pg_port
        self.pg_db = pg_db
        self.pg_user = pg_user
        self.pg_password = pg_password

    def execute(self, context):
        """
        Main logic:
         1) Create schema 'gold_layer' in PostgreSQL if not exists (psycopg2).
         2) Read Silver data from MinIO (Parquet).
         3) Aggregate data into a Gold DataFrame.
         4) Write the Gold data to MinIO (Parquet).
         5) Insert the aggregated data into PostgreSQL ('gold_layer.brewery_summary').
        """
        self._create_schema_with_psycopg2("gold_layer")

        spark = self._get_spark_session()
        try:
            silver_path = f"s3a://{self.source_bucket}/silver_data"
            df_silver = spark.read.parquet(silver_path)
            df_gold = self._create_gold_view(df_silver)

            gold_path = f"s3a://{self.destination_bucket}/gold_data"
            df_gold.write.mode("overwrite").parquet(gold_path)
            self.log.info(f"Gold data written to {gold_path}")

            self._write_gold_to_postgres(df_gold)
        finally:
            spark.stop()

    def _create_schema_with_psycopg2(self, schema_name):
        """
        Uses psycopg2 to connect to PostgreSQL and create the schema if it doesn't exist.
        This avoids potential permission or feature issues by letting Spark attempt it directly.
        """
        try:
            conn = psycopg2.connect(
                host=self.pg_host,
                port=self.pg_port,
                dbname=self.pg_db,
                user=self.pg_user,
                password=self.pg_password
            )
            conn.set_session(autocommit=True)
            cur = conn.cursor()
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
            cur.close()
            conn.close()
            self.log.info(f"Schema '{schema_name}' ensured in PostgreSQL.")
        except Exception as e:
            self.log.error(f"Error creating schema {schema_name}: {e}")
            raise

    def _get_spark_session(self):
        """
        Instantiates a SparkSession that can read from MinIO and write to PostgreSQL
        using the org.postgresql driver.
        """
        return (
            SparkSession.builder
            .appName("ProcessGoldLayer")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,org.postgresql:postgresql:42.2.18")
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{self.minio_endpoint}")
            .config("spark.hadoop.fs.s3a.access.key", self.minio_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.minio_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
        )

    @staticmethod
    def _create_gold_view(df):
        """
        Aggregates the data by brewery_type and state, computing
        the total number of breweries (brewery_count).
        """
        return df.groupBy("brewery_type", "state").agg(count("id").alias("brewery_count"))

    def _write_gold_to_postgres(self, df):
        """
        Writes the aggregated DataFrame to PostgreSQL in the table 'gold_layer.brewery_summary',
        overwriting any existing data.
        """
        jdbc_url = f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_db}"
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "gold_layer.brewery_summary") \
            .option("user", self.pg_user) \
            .option("password", self.pg_password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()

        self.log.info("Gold data written to PostgreSQL in table 'gold_layer.brewery_summary'.")
