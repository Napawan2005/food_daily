# Refactor Notes — `dags/`

## `dags/miniO.py`

- **Duplicate function definition**: `load_and_validate_csv` is defined twice (once around line 17, once around line 26). The second definition silently overrides the first.
- **Wrong return value**: the second `load_and_validate_csv` returns `path` (a `str`) instead of `df` (the validated `pd.DataFrame`), even though the type hint says `-> pd.DataFrame`. Any caller expecting a DataFrame will break.
- **Unused import**: `clickhouse_connect` is imported but never used in this file.
- **Dead/commented code**: `# path SOURCE_CSV = "/opt/airflow/dataset/food_daily.csv"` should be removed or turned into a real constant.
- **File name doesn't match contents**: `miniO.py` mixes MinIO client helpers with CSV validation logic. Consider splitting:
  - `minio_client.py` → `get_minio_client`, `list_minio_buckets`
  - `csv_utils.py` → `load_and_validate_csv`
- **Duplicated logic across files**: `load_and_validate_csv` here overlaps with `extract_validate_csv` in `dags/food_daily.py` (real pipeline) — the two should be unified into one shared implementation instead of maintained separately.

## `dags/food_daily.py` (the actual pipeline, `POC_food_daily_pipeline`)

- **Hardcoded connection strings duplicated with `docker-compose.yml`**: `MINIO_ENDPOINT` default, `CLICKHOUSE_HOST`/`PORT` defaults, bucket names, S3 URL construction (`http://miniO:9000/...`) — worth centralizing as shared config/constants instead of repeating literals inline.
- **`minio_client()` and `get_minio_client()` in `miniO.py` are two separate implementations of the same thing** — should be a single shared helper.
- **`start_date=datetime(2026, 8, 1)`** is a fixed future date hardcoded in the DAG definition — worth double-checking this is intentional before scheduling goes live.
- **Local temp file naming**: `f"/tmp/food_daily_{ds}.parquet"` — no collision protection if two runs for different `ds` overlap; also relies on the container's `/tmp` persisting only for the task duration (fine for now, but worth a comment on why it's safe).
- **Credentials passed directly into SQL string** (`client.command(f"...'{MINIO_ACCESS_KEY}', '{MINIO_SECRET_KEY}'...")`) — works for ClickHouse's `s3()` table function but means secrets end up in query logs; consider whether ClickHouse named collections/secrets could avoid this.
- **No shared module for DB/S3 clients** between `food_daily.py` and `miniO.py` — both re-implement client construction with slightly different signatures (`minio_client()` reads globals, `get_minio_client()` takes args).

## `dags/test.py`

- **Imports from `miniO.py` directly** (`from miniO import ...`) rather than the pipeline in `food_daily.py` — once `miniO.py` is split/cleaned up, update this import.
- **Purpose unclear from name**: `test.py` isn't a pytest test file, it's a second DAG (`food_daily`) that just checks the MinIO connection. Consider renaming to something like `check_minio_connection_dag.py` so it isn't mistaken for a test suite, and to avoid ambiguity with real tests if a `tests/` folder is added later.

## Suggested target structure

```
dags/
  food_daily_pipeline.py     # the DAG in food_daily.py, renamed for clarity
  minio_connection_check.py  # the DAG in test.py, renamed for clarity
  common/
    minio_client.py          # single get_minio_client()
    csv_utils.py             # single load_and_validate_csv()
    clickhouse_utils.py      # client creation + table DDL, shared if a 2nd DAG needs it
```
