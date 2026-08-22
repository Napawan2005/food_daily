import sys
import types
from pathlib import Path
from unittest import mock

import pytest

DAGS_DIR = Path(__file__).resolve().parent.parent
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))


@pytest.fixture
def food_daily_module(monkeypatch):
    """Import dags/food_daily.py with MinIO and ClickHouse fully mocked.

    food_daily.py connects to MinIO and ClickHouse at *import time*
    (module-level code), so those dependencies must be stubbed out in
    sys.modules before the import happens.
    """
    monkeypatch.setitem(
        sys.modules, "dotenv", types.SimpleNamespace(load_dotenv=lambda *a, **k: None)
    )

    fake_minio_client = mock.Mock(name="minio_client")
    fake_minio_module = types.SimpleNamespace(
        get_minio_client=mock.Mock(return_value=fake_minio_client),
        list_minio_buckets=mock.Mock(
            return_value=["raw-food-daily", "parquet-food-daily"]
        ),
    )
    monkeypatch.setitem(sys.modules, "minio_client", fake_minio_module)

    fake_ch_client = mock.Mock(name="clickhouse_client")
    fake_ch_module = types.SimpleNamespace(
        get_client=mock.Mock(return_value=fake_ch_client)
    )
    monkeypatch.setitem(sys.modules, "clickhouse_connect", fake_ch_module)

    sys.modules.pop("food_daily", None)
    import food_daily as fd  # noqa: E402

    yield fd

    sys.modules.pop("food_daily", None)


@pytest.fixture
def food_daily_tasks(food_daily_module):
    """Build the DAG and return {task_id: python_callable}."""
    dag_obj = food_daily_module.food_daily()
    return {
        task_id: task.python_callable
        for task_id, task in dag_obj.task_dict.items()
    }
