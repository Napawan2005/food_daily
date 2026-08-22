import pandas as pd
import pytest


EXPECTED_TASK_IDS = {
    "check_minio_connection",
    "ensure_bucket_exists",
    "extract_validate_csv",
    "upload_raw_to_minio",
    "convert_csv_to_parquet",
    "check_connection_clickhouse",
    "create_food_daily_table",
    "load_data_to_food_daily",
}


def test_dag_builds_all_expected_tasks(food_daily_tasks):
    assert set(food_daily_tasks) == EXPECTED_TASK_IDS


def test_extract_validate_csv_passes_when_all_columns_present(
    food_daily_module, food_daily_tasks, tmp_path, monkeypatch
):
    csv_path = tmp_path / "food_daily.csv"
    pd.DataFrame([{col: "x" for col in food_daily_module.EXPECTED_COLUMNS}]).to_csv(
        csv_path, index=False
    )
    monkeypatch.setattr(food_daily_module, "SOURCE_CSV", str(csv_path))

    result = food_daily_tasks["extract_validate_csv"]()

    assert result == str(csv_path)


def test_extract_validate_csv_raises_when_column_missing(
    food_daily_module, food_daily_tasks, tmp_path, monkeypatch
):
    columns = [c for c in food_daily_module.EXPECTED_COLUMNS if c != "ratings"]
    csv_path = tmp_path / "food_daily.csv"
    pd.DataFrame([{col: "x" for col in columns}]).to_csv(csv_path, index=False)
    monkeypatch.setattr(food_daily_module, "SOURCE_CSV", str(csv_path))

    with pytest.raises(ValueError, match="ratings"):
        food_daily_tasks["extract_validate_csv"]()


def test_check_minio_connection_returns_true_on_success(food_daily_tasks):
    assert food_daily_tasks["check_minio_connection"]() is True


def test_check_minio_connection_returns_false_on_failure(
    food_daily_module, food_daily_tasks
):
    food_daily_module.list_minio_buckets.side_effect = RuntimeError("boom")

    assert food_daily_tasks["check_minio_connection"]() is False


def test_ensure_bucket_exists_creates_missing_buckets_only(
    food_daily_module, food_daily_tasks
):
    food_daily_module.list_minio_buckets.return_value = ["raw-food-daily"]

    food_daily_tasks["ensure_bucket_exists"]()

    food_daily_module.client.create_bucket.assert_called_once_with(
        Bucket=food_daily_module.PARQUET_BUCKET
    )


def test_ensure_bucket_exists_creates_nothing_when_both_present(
    food_daily_module, food_daily_tasks
):
    food_daily_module.list_minio_buckets.return_value = [
        food_daily_module.RAW_BUCKET,
        food_daily_module.PARQUET_BUCKET,
    ]

    food_daily_tasks["ensure_bucket_exists"]()

    food_daily_module.client.create_bucket.assert_not_called()


def test_upload_raw_to_minio_uploads_with_dated_key(
    food_daily_module, food_daily_tasks
):
    key = food_daily_tasks["upload_raw_to_minio"]("/some/local.csv")

    assert key.endswith("/food_daily.csv")
    food_daily_module.client.upload_file.assert_called_once_with(
        "/some/local.csv", food_daily_module.RAW_BUCKET, key
    )


def test_convert_csv_to_parquet_uploads_and_cleans_up_temp_files(
    food_daily_module, food_daily_tasks, tmp_path, monkeypatch
):
    raw_csv = tmp_path / "raw.csv"
    pd.DataFrame([{c: "x" for c in food_daily_module.EXPECTED_COLUMNS}]).to_csv(
        raw_csv, index=False
    )

    def fake_download_file(bucket, key, dest):
        raw_csv.replace(dest)

    food_daily_module.client.download_file.side_effect = fake_download_file
    monkeypatch.chdir(tmp_path)

    key = food_daily_tasks["convert_csv_to_parquet"]("2026-08-22/food_daily.csv")

    assert key.endswith("/food_daily.parquet")
    food_daily_module.client.upload_file.assert_called_once()
    uploaded_local_path = food_daily_module.client.upload_file.call_args[0][0]
    assert not food_daily_module.os.path.exists(uploaded_local_path)


def test_convert_csv_to_parquet_raises_runtime_error_on_upload_failure(
    food_daily_module, food_daily_tasks, tmp_path
):
    raw_csv = tmp_path / "raw.csv"
    pd.DataFrame([{c: "x" for c in food_daily_module.EXPECTED_COLUMNS}]).to_csv(
        raw_csv, index=False
    )

    def fake_download_file(bucket, key, dest):
        raw_csv.replace(dest)

    food_daily_module.client.download_file.side_effect = fake_download_file
    food_daily_module.client.upload_file.side_effect = Exception("network down")

    with pytest.raises(RuntimeError, match="Upload parquet to MinIO failed"):
        food_daily_tasks["convert_csv_to_parquet"]("2026-08-22/food_daily.csv")


def test_check_connection_clickhouse_returns_true_on_success(food_daily_tasks):
    assert food_daily_tasks["check_connection_clickhouse"]() is True


def test_check_connection_clickhouse_returns_false_on_failure(
    food_daily_module, food_daily_tasks
):
    food_daily_module.ch_client.command.side_effect = Exception("no route")

    assert food_daily_tasks["check_connection_clickhouse"]() is False


def test_create_food_daily_table_drops_then_creates(
    food_daily_module, food_daily_tasks
):
    food_daily_tasks["create_food_daily_table"]()

    commands = [c.args[0] for c in food_daily_module.ch_client.command.call_args_list]
    assert "DROP TABLE IF EXISTS food_daily" in commands[0]
    assert "CREATE TABLE food_daily" in commands[1]


def test_create_food_daily_table_wraps_errors_in_runtime_error(
    food_daily_module, food_daily_tasks
):
    food_daily_module.ch_client.command.side_effect = Exception("syntax error")

    with pytest.raises(RuntimeError, match="Failed to create table food_daily"):
        food_daily_tasks["create_food_daily_table"]()


def test_load_data_to_food_daily_inserts_from_s3_and_returns_count(
    food_daily_module, food_daily_tasks
):
    food_daily_module.ch_client.command.side_effect = [None, 42]

    result = food_daily_tasks["load_data_to_food_daily"]("2026-08-22/food_daily.parquet")

    assert result == 42
    insert_sql = food_daily_module.ch_client.command.call_args_list[0].args[0]
    assert "INSERT INTO food_daily" in insert_sql
    assert "2026-08-22/food_daily.parquet" in insert_sql


def test_load_data_to_food_daily_wraps_errors_in_runtime_error(
    food_daily_module, food_daily_tasks
):
    food_daily_module.ch_client.command.side_effect = Exception("s3 unreachable")

    with pytest.raises(RuntimeError, match="Failed to load parquet"):
        food_daily_tasks["load_data_to_food_daily"]("2026-08-22/food_daily.parquet")
