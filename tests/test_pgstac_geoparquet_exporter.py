import os
import sys
from unittest.mock import Mock, patch, mock_open, MagicMock

import pytest

# Mock heavy dependencies
sys.modules["stac_geoparquet"] = Mock()
sys.modules["stac_geoparquet.pgstac_reader"] = Mock()

from pgstac_geoparquet_exporter.__main__ import main  # noqa: E402


@pytest.fixture
def base_env():
    return {
        "PGHOST": "testhost",
        "PGDATABASE": "testdb",
        "PGUSER": "testuser",
        "PGPASSWORD": "testpass",
    }


@patch("pgstac_geoparquet_exporter.__main__.CollectionConfig")
@patch("pathlib.Path.mkdir")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: sentinel-2\n    partition_frequency: YS\n",
)
def test_yearly_partition_creates_collection_config(
    mock_file, mock_mkdir, mock_config_class, base_env
):
    """Verify yearly partition creates CollectionConfig with correct parameters"""
    mock_config_instance = MagicMock()
    mock_config_instance.export_collection.return_value = [
        "/output/sentinel-2/part-0.parquet"
    ]
    mock_config_class.return_value = mock_config_instance

    base_env["EXPORT_MODE"] = "complete"

    with patch.dict(os.environ, base_env, clear=True):
        main()

    # Verify CollectionConfig was created with correct parameters
    mock_config_class.assert_called_once()
    call_kwargs = mock_config_class.call_args[1]
    assert call_kwargs["collection_id"] == "sentinel-2"
    assert call_kwargs["partition_frequency"] == "YS"
    assert "stac_api" in call_kwargs
    assert "should_inject_dynamic_properties" in call_kwargs


@patch("pgstac_geoparquet_exporter.__main__.CollectionConfig")
@patch("pathlib.Path.mkdir")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: sentinel-2\n    partition_frequency: YS\n",
)
def test_export_collection_called_with_correct_params(
    mock_file, mock_mkdir, mock_config_class, base_env
):
    """Verify export_collection is called with correct parameters"""
    mock_config_instance = MagicMock()
    mock_config_instance.export_collection.return_value = [
        "/output/sentinel-2/part-0.parquet"
    ]
    mock_config_class.return_value = mock_config_instance

    base_env["EXPORT_MODE"] = "complete"

    with patch.dict(os.environ, base_env, clear=True):
        main()

    # Verify export_collection was called
    mock_config_instance.export_collection.assert_called_once()
    call_kwargs = mock_config_instance.export_collection.call_args[1]

    assert "conninfo" in call_kwargs
    assert "testhost" in call_kwargs["conninfo"]
    assert "testdb" in call_kwargs["conninfo"]
    assert call_kwargs["output_protocol"] == "file"
    assert call_kwargs["output_path"] == "/output/sentinel-2"
    assert call_kwargs["storage_options"] == {}


@patch("pgstac_geoparquet_exporter.__main__.CollectionConfig")
@patch("pathlib.Path.mkdir")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: landsat\n",
)
def test_no_partition_creates_single_file(
    mock_file, mock_mkdir, mock_config_class, base_env
):
    """Verify collection without partition_frequency creates single file"""
    mock_config_instance = MagicMock()
    mock_config_instance.export_collection.return_value = [
        "/output/landsat/items.parquet"
    ]
    mock_config_class.return_value = mock_config_instance

    base_env["EXPORT_MODE"] = "complete"

    with patch.dict(os.environ, base_env, clear=True):
        main()

    # Verify CollectionConfig was created without partition_frequency
    call_kwargs = mock_config_class.call_args[1]
    assert call_kwargs["collection_id"] == "landsat"
    assert call_kwargs["partition_frequency"] is None


@patch("pgstac_geoparquet_exporter.__main__.CollectionConfig")
@patch("pathlib.Path.mkdir")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: col1\n  - name: col2\n",
)
def test_multiple_collections_all_exported(
    mock_file, mock_mkdir, mock_config_class, base_env
):
    """Verify all collections in config are exported"""
    mock_config_instance = MagicMock()
    mock_config_instance.export_collection.return_value = ["/output/col1/items.parquet"]
    mock_config_class.return_value = mock_config_instance

    base_env["EXPORT_MODE"] = "complete"

    with patch.dict(os.environ, base_env, clear=True):
        main()

    assert mock_config_class.call_count == 2
    collections = [
        call[1]["collection_id"] for call in mock_config_class.call_args_list
    ]
    assert "col1" in collections
    assert "col2" in collections


@patch("builtins.open", new_callable=mock_open, read_data="collections: []\n")
def test_incremental_mode_not_supported(mock_file, base_env):
    """Verify incremental mode returns error code"""
    base_env["EXPORT_MODE"] = "incremental"

    with patch.dict(os.environ, base_env, clear=True):
        result = main()

    assert result == 1


@patch("pgstac_geoparquet_exporter.__main__.CollectionConfig")
@patch("pathlib.Path.mkdir")
@patch(
    "builtins.open", new_callable=mock_open, read_data="collections:\n  - name: test\n"
)
def test_connection_string_format(mock_file, mock_mkdir, mock_config_class, base_env):
    """Verify PostgreSQL connection string contains all required parts"""
    mock_config_instance = MagicMock()
    mock_config_instance.export_collection.return_value = ["/output/test/items.parquet"]
    mock_config_class.return_value = mock_config_instance

    base_env["EXPORT_MODE"] = "complete"
    base_env["PGPORT"] = "5433"

    with patch.dict(os.environ, base_env, clear=True):
        main()

    conninfo = mock_config_instance.export_collection.call_args[1]["conninfo"]
    assert "host=testhost" in conninfo
    assert "port=5433" in conninfo
    assert "dbname=testdb" in conninfo
    assert "user=testuser" in conninfo
    assert "password=testpass" in conninfo


@patch("pgstac_geoparquet_exporter.__main__.CollectionConfig")
@patch("pathlib.Path.mkdir")
@patch(
    "builtins.open", new_callable=mock_open, read_data="collections:\n  - name: test\n"
)
def test_pgport_defaults_to_5432(mock_file, mock_mkdir, mock_config_class, base_env):
    """Verify PGPORT defaults to 5432 when not set"""
    mock_config_instance = MagicMock()
    mock_config_instance.export_collection.return_value = ["/output/test/items.parquet"]
    mock_config_class.return_value = mock_config_instance

    base_env["EXPORT_MODE"] = "complete"
    # Don't set PGPORT

    with patch.dict(os.environ, base_env, clear=True):
        main()

    conninfo = mock_config_instance.export_collection.call_args[1]["conninfo"]
    assert "port=5432" in conninfo


@patch("pgstac_geoparquet_exporter.__main__.CollectionConfig")
@patch("pathlib.Path.mkdir")
@patch(
    "builtins.open", new_callable=mock_open, read_data="collections:\n  - name: test\n"
)
def test_custom_output_path(mock_file, mock_mkdir, mock_config_class, base_env):
    """Verify OUTPUT_PATH env var is used"""
    mock_config_instance = MagicMock()
    mock_config_instance.export_collection.return_value = [
        "/custom/output/test/items.parquet"
    ]
    mock_config_class.return_value = mock_config_instance

    base_env["EXPORT_MODE"] = "complete"
    base_env["OUTPUT_PATH"] = "/custom/output"

    with patch.dict(os.environ, base_env, clear=True):
        main()

    output_path = mock_config_instance.export_collection.call_args[1]["output_path"]
    assert output_path == "/custom/output/test/items.parquet"


@patch("pgstac_geoparquet_exporter.__main__.CollectionConfig")
@patch("pathlib.Path.mkdir")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: test\n",
)
def test_custom_config_path(mock_file, mock_mkdir, mock_config_class, base_env):
    """Verify CONFIG_PATH env var is used"""
    mock_config_instance = MagicMock()
    mock_config_instance.export_collection.return_value = ["/output/test/items.parquet"]
    mock_config_class.return_value = mock_config_instance

    base_env["EXPORT_MODE"] = "complete"
    base_env["CONFIG_PATH"] = "/custom/config.yaml"

    with patch.dict(os.environ, base_env, clear=True):
        main()

    mock_file.assert_called_with("/custom/config.yaml")


@patch("pgstac_geoparquet_exporter.__main__.CollectionConfig")
@patch("pathlib.Path.mkdir")
@patch(
    "builtins.open", new_callable=mock_open, read_data="collections:\n  - name: test\n"
)
def test_returns_zero_on_success(mock_file, mock_mkdir, mock_config_class, base_env):
    """Verify main returns 0 on successful execution"""
    mock_config_instance = MagicMock()
    mock_config_instance.export_collection.return_value = ["/output/test/items.parquet"]
    mock_config_class.return_value = mock_config_instance

    base_env["EXPORT_MODE"] = "complete"

    with patch.dict(os.environ, base_env, clear=True):
        assert main() == 0


@patch("pgstac_geoparquet_exporter.__main__.CollectionConfig")
@patch("pathlib.Path.mkdir")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: test\n    inject_dynamic_properties: true\n    rewrite: true\n    skip_empty_partitions: false\n",
)
def test_collection_config_options(mock_file, mock_mkdir, mock_config_class, base_env):
    """Verify collection config options are passed correctly"""
    mock_config_instance = MagicMock()
    mock_config_instance.export_collection.return_value = ["/output/test/items.parquet"]
    mock_config_class.return_value = mock_config_instance

    base_env["EXPORT_MODE"] = "complete"

    with patch.dict(os.environ, base_env, clear=True):
        main()

    # Check CollectionConfig creation
    config_kwargs = mock_config_class.call_args[1]
    assert config_kwargs["should_inject_dynamic_properties"] is True

    # Check export_collection call
    export_kwargs = mock_config_instance.export_collection.call_args[1]
    assert export_kwargs["rewrite"] is True
    assert export_kwargs["skip_empty_partitions"] is False


@patch("pgstac_geoparquet_exporter.__main__.CollectionConfig")
@patch("pathlib.Path.mkdir")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: test\n",
)
def test_custom_stac_api_url(mock_file, mock_mkdir, mock_config_class, base_env):
    """Verify STAC_API_URL env var is used"""
    mock_config_instance = MagicMock()
    mock_config_instance.export_collection.return_value = ["/output/test/items.parquet"]
    mock_config_class.return_value = mock_config_instance

    base_env["EXPORT_MODE"] = "complete"
    base_env["STAC_API_URL"] = "https://custom.api/stac/v1"

    with patch.dict(os.environ, base_env, clear=True):
        main()

    config_kwargs = mock_config_class.call_args[1]
    assert config_kwargs["stac_api"] == "https://custom.api/stac/v1"


@patch("pgstac_geoparquet_exporter.__main__.CollectionConfig")
@patch("pathlib.Path.mkdir")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: test\n    partition_frequency: MS\n",
)
def test_monthly_partition_frequency(
    mock_file, mock_mkdir, mock_config_class, base_env
):
    """Verify monthly partition frequency is supported"""
    mock_config_instance = MagicMock()
    mock_config_instance.export_collection.return_value = [
        "/output/test/part-0.parquet"
    ]
    mock_config_class.return_value = mock_config_instance

    base_env["EXPORT_MODE"] = "complete"

    with patch.dict(os.environ, base_env, clear=True):
        main()

    config_kwargs = mock_config_class.call_args[1]
    assert config_kwargs["partition_frequency"] == "MS"
