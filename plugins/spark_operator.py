from datetime import datetime
from airflow.models import BaseOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lower, trim, coalesce
from minio import Minio

class SparkMinIOSession:
    def __init__(self, minio_endpoint, minio_access_key, minio_secret_key):
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key

    def get_spark_session(self):
        return SparkSession.builder \
            .appName("SparkMinIOSession") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.563") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{self.minio_endpoint}") \
            .config("spark.hadoop.fs.s3a.access.key", self.minio_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", self.minio_secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.impl.disable.cache", "true") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.driver.memory", "8g") \
            .config("spark.executor.memory", "8g") \
            .config("spark.driver.maxResultSize", "2g") \
            .config("spark.python.worker.memory", "1g") \
            .config("spark.rpc.message.maxSize", "1024") \
            .master("local[*]") \
            .getOrCreate()

class SilverLayerOperator(BaseOperator):
    def __init__(self, source_bucket, dest_bucket, minio_endpoint, minio_access_key, minio_secret_key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_bucket = source_bucket
        self.dest_bucket = dest_bucket
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key

    def execute(self, context):
        try:
            self.log.info(f"MinIO endpoint: {self.minio_endpoint}")
            self.log.info(f"Source bucket: {self.source_bucket}")
            self.log.info(f"Destination bucket: {self.dest_bucket}")

            minio_client = Minio(
                endpoint=self.minio_endpoint,
                access_key=self.minio_access_key,
                secret_key=self.minio_secret_key,
                secure=False
            )

            objects = list(minio_client.list_objects(self.source_bucket, recursive=True))
            if not objects:
                self.log.warning(f"No files found in s3a://{self.source_bucket}/")
                return

            self.log.info(f"Found {len(objects)} objects in the bucket")

            spark_session = SparkMinIOSession(
                self.minio_endpoint,
                self.minio_access_key,
                self.minio_secret_key
            ).get_spark_session()

            self.log.info("SparkSession created successfully")

            source_path = f"s3a://{self.source_bucket}/"
            self.log.info(f"Attempting to read from {source_path}")

            df = spark_session.read.option("recursiveFileLookup", "true").json(source_path)
            self.log.info(f"Successfully read {df.count()} records from source")

            df_silver = df.select(
                col("id"),
                trim(col("name")).alias("name"),
                lower(col("brewery_type")).alias("brewery_type"),
                concat_ws(", ", col("address_1"), col("address_2"), col("address_3")).alias("full_address"),
                trim(col("city")).alias("city"),
                coalesce(col("state_province"), col("state")).alias("state"),
                col("postal_code"),
                trim(col("country")).alias("country"),
                col("longitude").cast("double"),
                col("latitude").cast("double"),
                col("phone"),
                col("website_url")
            )

            silver_path = f"s3a://{self.dest_bucket}/"
            if not silver_path or silver_path == "s3a://":
                raise ValueError(f"Invalid silver path: '{silver_path}'")

            self.log.info(f"Silver path: {silver_path}")

            # Generate filename with current timestamp
            current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"breweries_{current_date}.parquet"

            df_silver.repartition(1) \
                .write \
                .mode("overwrite") \
                .parquet(f"{silver_path}/{file_name}")

            self.log.info(f"Silver transformation completed with {df_silver.count()} records. Data written to {silver_path}/{file_name}")

        except Exception as e:
            self.log.error(f"Error in SilverLayerOperator: {str(e)}")
            raise
        finally:
            if 'spark_session' in locals():
                spark_session.stop()

class WriteSilverLayerOperator(BaseOperator):
    def __init__(self, dest_bucket, minio_endpoint, minio_access_key, minio_secret_key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dest_bucket = dest_bucket
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key

    def execute(self, context):
        spark_session = SparkMinIOSession(
            self.minio_endpoint,
            self.minio_access_key,
            self.minio_secret_key
        ).get_spark_session()

        try:
            silver_path = f"s3a://{self.dest_bucket}/"
            df_silver = spark_session.read.parquet(silver_path)

            dest_path = f"s3a://{self.dest_bucket}/brewery_parquet/"
            df_silver.write \
                .partitionBy("state") \
                .mode("overwrite") \
                .parquet(dest_path)

            self.log.info(f"Successfully wrote {df_silver.count()} records to Silver layer in Parquet format.")

        except Exception as e:
            self.log.error(f"Error in WriteSilverLayerOperator: {str(e)}")
            raise
        finally:
            spark_session.stop()