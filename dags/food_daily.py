from __future__ import annotations
import os
import uuid
from datetime import datetime

import pandas as pd

from dotenv import load_dotenv
from airflow.sdk import dag, task
from minio_client import get_minio_client, list_minio_buckets

import clickhouse_connect

load_dotenv()
MINIO_ENDPOINT =  os.getenv("MINIO_ENDPOINT", "http://miniO:9000")
MINIO_ACCESS_KEY =  os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY =  os.getenv("MINIO_SECRET_KEY")
SOURCE_CSV = "/opt/airflow/dataset/food_daily.csv"

RAW_BUCKET = "raw-food-daily"
PARQUET_BUCKET = "parquet-food-daily"

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse_db")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB")
ch_client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DB,
        )

client = get_minio_client(
    os.getenv("MINIO_ENDPOINT"),
    os.getenv("MINIO_ACCESS_KEY"),
    os.getenv("MINIO_SECRET_KEY"),
)
buckets = list_minio_buckets(client)


EXPECTED_COLUMNS = [
    "Customer_id", "date", "time", "order_id", "items",
    "amount", "mode", "restaurnt", "Status", "ratings", "feedback",
]
        

@dag(
    
    dag_id="food_daily_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["food_daily", "minio", "clickhouse"],
    
)
def food_daily():
    
    @task
    def check_minio_connection() -> bool:
        try:
            client = get_minio_client(
                os.getenv("MINIO_ENDPOINT"),
                os.getenv("MINIO_ACCESS_KEY"),
                os.getenv("MINIO_SECRET_KEY"),
            )
            buckets = list_minio_buckets(client)
        except Exception as e:
            print(f"Cannot connect to MinIO: {e}")
            return False

        print(f"connection ok, buckets: {buckets}")
        return True
    
    @task
    def ensure_bucket_exists( )-> None:
        existing = list_minio_buckets(client)
        if RAW_BUCKET not in existing:
            client.create_bucket(Bucket=RAW_BUCKET)
            print("create BUCKET {RAW_BUCKET} !!")
        if PARQUET_BUCKET not in existing:
            client.create_bucket(Bucket=PARQUET_BUCKET)
            print("create BUCKET {RAW_BUCKET}  !!")
        
        print("you have bucket !!")

    @task
    def extract_validate_csv() -> str:
        df = pd.read_csv(SOURCE_CSV , encoding = "utf-8-sig")
        missing = set(EXPECTED_COLUMNS) - set(df.columns)
        if missing:
            raise  ValueError(f"CSV missing expected columns: {missing}")
        return SOURCE_CSV
    
    
    @task
    def upload_raw_to_minio( csv_path: str) -> str:
        ds = datetime.today().strftime("%Y-%m-%d")
        key = f"{ds}/food_daily.csv"
        client.upload_file(csv_path, RAW_BUCKET, key)
        return key
    
    
    @task
    def convert_csv_to_parquet (raw_key: str)->str:
        ds = datetime.today().strftime("%Y-%m-%d")
        run_uid = uuid.uuid4().hex

        local_raw = f"/tmp/food_daily_raw_{ds}_{run_uid}.csv"
        client.download_file(RAW_BUCKET, raw_key, local_raw)

        df = pd.read_csv(local_raw, encoding="utf-8-sig")

        local_part = f"/tmp/food_daily_{ds}_{run_uid}.parquet"
        key = f"{ds}/food_daily.parquet"
        df.to_parquet(local_part , index=False)
        try:
            client.upload_file(local_part, PARQUET_BUCKET , key)
        except Exception as e:
            raise RuntimeError(f"Upload parquet to MinIO failed: {key}") from e
        finally:
            if os.path.exists(local_part):
                os.remove(local_part)
            if os.path.exists(local_raw):
                os.remove(local_raw)
        return key
    
    @task
    def check_connection_clickhouse() -> None:
        try:
            ch_client.command("SELECT 1")
            
        except Exception as e:
            print(f"Cannot connect to Clickhouse: {e}")
            return False
        print("clickhouse connect ok!")
        return True
    
    
    
    @task
    def create_food_daily_table() -> None:
        try:
            ch_client.command("""
               DROP TABLE IF EXISTS food_daily;
                              """)
            
            ch_client.command("""
                              
                CREATE TABLE food_daily(
                    Customer_id String PRIMARY KEY,
                    date String,
                    time String,
                    order_id String,
                    items Array(String),
                    amount Int,
                    mode String,
                    restaurnt String,
                    Status String,
                    ratings Nullable(Int),
                    feedback String,
                    
                ) ENGINE = MergeTree ORDER BY (Customer_id)

            """)
        except Exception as e :
            raise RuntimeError(f"Failed to create table food_daily: {e}") from e
        print("create Table food_daily success !!")
    
    @task
    def load_data_to_food_daily(parquet_key: str) -> int:
        s3_url = f"http://miniO:9000/{PARQUET_BUCKET}/{parquet_key}"
        try:
            ch_client.command(f"""
                INSERT INTO food_daily
                (Customer_id, date, time, order_id, items, amount, mode, restaurnt, Status, ratings, feedback)
                SELECT Customer_id, date , time, order_id, [items], amount, mode, restaurnt, Status, ratings, feedback
                FROM s3('{s3_url}' , '{MINIO_ACCESS_KEY}' , '{MINIO_SECRET_KEY}' , 'Parquet')
             """)
        except Exception as e:
            raise RuntimeError(f"Failed to load parquet '{parquet_key}' into ClickHouse: {e}") from e
        return ch_client.command("SELECT count() FROM food_daily")
        
    
    connect_miniO = check_minio_connection()
    csv_path = extract_validate_csv()
    bucket_ready = ensure_bucket_exists()
    key_miniO_load_csv = upload_raw_to_minio(csv_path)
    key_miniO_load_parquet = convert_csv_to_parquet(key_miniO_load_csv)

    connect_miniO >> bucket_ready
    [csv_path, bucket_ready] >> key_miniO_load_csv >> key_miniO_load_parquet >> check_connection_clickhouse() >> create_food_daily_table() >> load_data_to_food_daily(key_miniO_load_parquet)
    
    
food_daily()
