"""Daily ETL: Frankfurter API -> transform -> MongoDB. Data passes between tasks via named XComs."""
import os
from datetime import datetime, timedelta

from pymongo import MongoClient, UpdateOne

try:  # Airflow 3 / Astro Runtime 3.x
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task

from include.etl_helpers import fetch_rates, transform_rates, utc_now_iso

BASE_CURRENCY = os.getenv("BASE_CURRENCY", "EUR")
QUOTE_CURRENCIES = os.getenv("QUOTE_CURRENCIES", "USD,GBP,JPY")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@mongo:27017/?authSource=admin")
MONGO_DB = os.getenv("MONGO_DB", "currency_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "exchange_rates")


@dag(
    dag_id="currency_exchange_etl",
    description="Daily currency exchange rates -> MongoDB",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "mpg", "retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["etl", "currency", "mongodb"],
)
def currency_exchange_etl():
    @task(task_id="extract_exchange_rates")
    def extract(ti=None):
        rows = fetch_rates(BASE_CURRENCY, QUOTE_CURRENCIES)
        # Named XComs: explicit keys instead of relying on the default "return_value"
        ti.xcom_push(key="raw_rates", value=rows)
        ti.xcom_push(key="retrieved_at", value=utc_now_iso())
        print(f"Extracted {len(rows)} rows: {rows}")

    @task(task_id="transform_exchange_rate_data")
    def transform(ti=None):
        raw_rows = ti.xcom_pull(task_ids="extract_exchange_rates", key="raw_rates")
        retrieved_at = ti.xcom_pull(task_ids="extract_exchange_rates", key="retrieved_at")
        documents = transform_rates(raw_rows, retrieved_at)
        ti.xcom_push(key="transformed_rates", value=documents)
        print(f"Transformed {len(documents)} documents: {documents}")

    @task(task_id="load_data_into_mongodb")
    def load(ti=None):
        documents = ti.xcom_pull(task_ids="transform_exchange_rate_data", key="transformed_rates")
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        try:
            collection = client[MONGO_DB][MONGO_COLLECTION]
            # Upsert so re-running the DAG for the same day never creates duplicates
            operations = [
                UpdateOne(
                    {
                        "base_currency": d["base_currency"],
                        "target_currency": d["target_currency"],
                        "rate_date": d["rate_date"],
                        "source": d["source"],
                    },
                    {"$set": d},
                    upsert=True,
                )
                for d in documents
            ]
            result = collection.bulk_write(operations)
            print(f"Upserted={result.upserted_count} modified={result.modified_count}")
        finally:
            client.close()

    extract() >> transform() >> load()


currency_exchange_etl()
