# Airflow Currency ETL (Astro CLI + MongoDB + Mongo Express)

Daily pipeline: Frankfurter API -> transform -> MongoDB. Tasks pass data through named XComs
(`raw_rates`, `retrieved_at`, `transformed_rates`).

## Run
    astro dev start
- Airflow UI:     http://localhost:8080
- Mongo Express:  http://localhost:8081  (admin / admin123)
- Trigger DAG `currency_exchange_etl`, then open db `currency_db` -> collection `exchange_rates`.

## Layout
- dags/currency_exchange_etl.py   DAG with 3 tasks
- include/etl_helpers.py          fetch + transform logic
- docker-compose.override.yml     MongoDB + Mongo Express, network + persistent volume
- .env                            config / Mongo URI
