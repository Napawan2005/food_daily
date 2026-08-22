from unittest import mock

from minio_client import get_minio_client, list_minio_buckets


def test_get_minio_client_builds_s3_client_with_given_credentials():
    with mock.patch("minio_client.boto3") as boto3_mock:
        get_minio_client("http://minio:9000", "access", "secret")

    boto3_mock.client.assert_called_once_with(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="access",
        aws_secret_access_key="secret",
    )


def test_list_minio_buckets_returns_bucket_names():
    client = mock.Mock()
    client.list_buckets.return_value = {
        "Buckets": [{"Name": "raw-food-daily"}, {"Name": "parquet-food-daily"}]
    }

    result = list_minio_buckets(client)

    assert result == ["raw-food-daily", "parquet-food-daily"]


def test_list_minio_buckets_returns_empty_list_when_no_buckets():
    client = mock.Mock()
    client.list_buckets.return_value = {"Buckets": []}

    assert list_minio_buckets(client) == []
