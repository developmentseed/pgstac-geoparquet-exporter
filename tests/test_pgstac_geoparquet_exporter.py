import os
import sys
from unittest.mock import Mock, patch, mock_open

import pytest

# Mock heavy dependencies
sys.modules["stac_geoparquet"] = Mock()
sys.modules["stac_geoparquet.pgstac_reader"] = Mock()

from pgstac_geoparquet_exporter.__main__ import inject_stac_links, main  # noqa: E402


@pytest.fixture
def base_env():
    return {
        "PGHOST": "testhost",
        "PGDATABASE": "testdb",
        "PGUSER": "testuser",
        "PGPASSWORD": "testpass",
        "STAC_API_URL": "http://localhost:8000/stac/v1",
    }


@patch("pgstac_geoparquet_exporter.__main__.sync_pgstac_to_parquet")
@patch("pathlib.Path.mkdir")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: sentinel-2\n    partition_frequency: YS\n",
)
def test_yearly_partition_uses_sync_pgstac(mock_file, mock_mkdir, mock_sync, base_env):
    """Verify yearly partition uses sync_pgstac_to_parquet"""
    base_env["EXPORT_MODE"] = "complete"

    with patch.dict(os.environ, base_env, clear=True):
        main()

    # Verify sync_pgstac_to_parquet was called
    mock_sync.assert_called_once()
    call_kwargs = mock_sync.call_args[1]
    assert "testhost" in call_kwargs["conninfo"]
    assert call_kwargs["output_path"] == "/output/sentinel-2"
    assert call_kwargs["updated_after"] is None
    assert call_kwargs["chunk_size"] == 8192  # default


@patch("pgstac_geoparquet_exporter.__main__.pgstac_to_parquet")
@patch("pathlib.Path.mkdir")
@patch("pathlib.Path.exists")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: landsat\n",
)
def test_no_partition_creates_single_file(
    mock_file, mock_exists, mock_mkdir, mock_to_parquet, base_env
):
    """Verify collection without partition_frequency creates single file"""
    mock_exists.return_value = False
    base_env["EXPORT_MODE"] = "complete"

    with patch.dict(os.environ, base_env, clear=True):
        main()

    # Verify pgstac_to_parquet was called
    mock_to_parquet.assert_called_once()
    call_kwargs = mock_to_parquet.call_args[1]
    assert "testhost" in call_kwargs["conninfo"]
    assert call_kwargs["output_path"] == "/output/landsat/items.parquet"
    assert call_kwargs["collection"] == "landsat"
    assert call_kwargs["chunk_size"] == 8192  # default


@patch("pgstac_geoparquet_exporter.__main__.pgstac_to_parquet")
@patch("pathlib.Path.mkdir")
@patch("pathlib.Path.exists")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: col1\n  - name: col2\n",
)
def test_multiple_collections_all_exported(
    mock_file, mock_exists, mock_mkdir, mock_to_parquet, base_env
):
    """Verify all collections in config are exported"""
    mock_exists.return_value = False
    base_env["EXPORT_MODE"] = "complete"

    with patch.dict(os.environ, base_env, clear=True):
        main()

    assert mock_to_parquet.call_count == 2
    collections = [call[1]["collection"] for call in mock_to_parquet.call_args_list]
    assert "col1" in collections
    assert "col2" in collections


@patch("pgstac_geoparquet_exporter.__main__.sync_pgstac_to_parquet")
@patch("pathlib.Path.mkdir")
@patch("builtins.open", new_callable=mock_open, read_data="collections: []\n")
def test_incremental_mode_supported(mock_file, mock_mkdir, mock_sync, base_env):
    """Verify incremental mode is now supported"""
    base_env["EXPORT_MODE"] = "incremental"

    with patch.dict(os.environ, base_env, clear=True):
        result = main()

    assert result == 0
    # sync_pgstac_to_parquet not called because no collections
    mock_sync.assert_not_called()


@patch("pgstac_geoparquet_exporter.__main__.pgstac_to_parquet")
@patch("pathlib.Path.mkdir")
@patch("pathlib.Path.exists")
@patch(
    "builtins.open", new_callable=mock_open, read_data="collections:\n  - name: test\n"
)
def test_connection_string_format(
    mock_file, mock_exists, mock_mkdir, mock_to_parquet, base_env
):
    """Verify PostgreSQL connection string contains all required parts"""
    mock_exists.return_value = False
    base_env["EXPORT_MODE"] = "complete"
    base_env["PGPORT"] = "5433"

    with patch.dict(os.environ, base_env, clear=True):
        main()

    conninfo = mock_to_parquet.call_args[1]["conninfo"]
    assert "host=testhost" in conninfo
    assert "port=5433" in conninfo
    assert "dbname=testdb" in conninfo
    assert "user=testuser" in conninfo
    assert "password=testpass" in conninfo


@patch("pgstac_geoparquet_exporter.__main__.pgstac_to_parquet")
@patch("pathlib.Path.mkdir")
@patch("pathlib.Path.exists")
@patch(
    "builtins.open", new_callable=mock_open, read_data="collections:\n  - name: test\n"
)
def test_pgport_defaults_to_5432(
    mock_file, mock_exists, mock_mkdir, mock_to_parquet, base_env
):
    """Verify PGPORT defaults to 5432 when not set"""
    mock_exists.return_value = False
    base_env["EXPORT_MODE"] = "complete"
    # Don't set PGPORT

    with patch.dict(os.environ, base_env, clear=True):
        main()

    conninfo = mock_to_parquet.call_args[1]["conninfo"]
    assert "port=5432" in conninfo


@patch("pgstac_geoparquet_exporter.__main__.pgstac_to_parquet")
@patch("pathlib.Path.mkdir")
@patch("pathlib.Path.exists")
@patch(
    "builtins.open", new_callable=mock_open, read_data="collections:\n  - name: test\n"
)
def test_custom_output_path(
    mock_file, mock_exists, mock_mkdir, mock_to_parquet, base_env
):
    """Verify OUTPUT_PATH env var is used"""
    mock_exists.return_value = False
    base_env["EXPORT_MODE"] = "complete"
    base_env["OUTPUT_PATH"] = "/custom/output"

    with patch.dict(os.environ, base_env, clear=True):
        main()

    output_path = mock_to_parquet.call_args[1]["output_path"]
    assert output_path == "/custom/output/test/items.parquet"


@patch("pgstac_geoparquet_exporter.__main__.pgstac_to_parquet")
@patch("pathlib.Path.mkdir")
@patch("pathlib.Path.exists")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: test\n",
)
def test_custom_config_path(
    mock_file, mock_exists, mock_mkdir, mock_to_parquet, base_env
):
    """Verify CONFIG_PATH env var is used"""
    mock_exists.return_value = False
    base_env["EXPORT_MODE"] = "complete"
    base_env["CONFIG_PATH"] = "/custom/config.yaml"

    with patch.dict(os.environ, base_env, clear=True):
        main()

    mock_file.assert_called_with("/custom/config.yaml")


@patch("pgstac_geoparquet_exporter.__main__.pgstac_to_parquet")
@patch("pathlib.Path.mkdir")
@patch("pathlib.Path.exists")
@patch(
    "builtins.open", new_callable=mock_open, read_data="collections:\n  - name: test\n"
)
def test_returns_zero_on_success(
    mock_file, mock_exists, mock_mkdir, mock_to_parquet, base_env
):
    """Verify main returns 0 on successful execution"""
    mock_exists.return_value = False
    base_env["EXPORT_MODE"] = "complete"

    with patch.dict(os.environ, base_env, clear=True):
        assert main() == 0


@patch("pgstac_geoparquet_exporter.__main__.pgstac_to_parquet")
@patch("pathlib.Path.mkdir")
@patch("pathlib.Path.exists")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: test\n    rewrite: true\n    chunk_size: 16384\n",
)
def test_collection_config_options(
    mock_file, mock_exists, mock_mkdir, mock_to_parquet, base_env
):
    """Verify collection config options are passed correctly"""
    mock_exists.return_value = False
    base_env["EXPORT_MODE"] = "complete"

    with patch.dict(os.environ, base_env, clear=True):
        main()

    # Check pgstac_to_parquet call
    call_kwargs = mock_to_parquet.call_args[1]
    assert call_kwargs["chunk_size"] == 16384


@patch("pgstac_geoparquet_exporter.__main__.pgstac_to_parquet")
@patch("pathlib.Path.mkdir")
@patch("pathlib.Path.exists")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: test\n    rewrite: false\n",
)
def test_rewrite_false_skips_existing_file(
    mock_file, mock_exists, mock_mkdir, mock_to_parquet, base_env
):
    """Verify rewrite=false skips existing files"""
    mock_exists.return_value = True  # File exists
    base_env["EXPORT_MODE"] = "complete"

    with patch.dict(os.environ, base_env, clear=True):
        main()

    # Should not call pgstac_to_parquet
    mock_to_parquet.assert_not_called()


@patch("pgstac_geoparquet_exporter.__main__.sync_pgstac_to_parquet")
@patch("pathlib.Path.mkdir")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: test\n    partition_frequency: MS\n",
)
def test_monthly_partition_frequency(mock_file, mock_mkdir, mock_sync, base_env):
    """Verify monthly partition frequency is supported"""
    base_env["EXPORT_MODE"] = "complete"

    with patch.dict(os.environ, base_env, clear=True):
        main()

    # Verify sync_pgstac_to_parquet was called (partitioning handled by pgstac)
    mock_sync.assert_called_once()


@patch("pgstac_geoparquet_exporter.__main__.sync_pgstac_to_parquet")
@patch("pathlib.Path.mkdir")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: test\n",
)
def test_incremental_mode_with_collection(mock_file, mock_mkdir, mock_sync, base_env):
    """Verify incremental mode calls sync_pgstac_to_parquet"""
    base_env["EXPORT_MODE"] = "incremental"

    with patch.dict(os.environ, base_env, clear=True):
        result = main()

    assert result == 0
    mock_sync.assert_called_once()
    call_kwargs = mock_sync.call_args[1]
    assert call_kwargs["output_path"] == "/output/test"
    assert call_kwargs["updated_after"] is None


def test_unknown_mode_returns_error(base_env):
    """Verify unknown export mode returns error code"""
    base_env["EXPORT_MODE"] = "unknown"

    with patch.dict(os.environ, base_env, clear=True):
        with patch("builtins.open", mock_open(read_data="collections: []\n")):
            result = main()

    assert result == 1


@patch("builtins.open", new_callable=mock_open, read_data="collections: []\n")
def test_missing_stac_api_url_returns_error(mock_file):
    """Verify missing STAC_API_URL returns error code"""
    env = {
        "PGHOST": "testhost",
        "PGDATABASE": "testdb",
        "PGUSER": "testuser",
        "PGPASSWORD": "testpass",
        # STAC_API_URL is intentionally missing
        "EXPORT_MODE": "complete",
    }

    with patch.dict(os.environ, env, clear=True):
        result = main()

    assert result == 1


def test_inject_stac_links():
    """Verify link injection adds correct links to STAC items."""
    stac_api_url = "https://example.com/stac/v1"
    add_links = inject_stac_links(stac_api_url)

    item = {
        "id": "test-item-123",
        "collection": "sentinel-2",
        "type": "Feature",
        "geometry": {},
        "properties": {},
    }

    result = add_links(item)

    assert "links" in result
    assert len(result["links"]) == 4

    # Check each link type
    links_by_rel = {link["rel"]: link["href"] for link in result["links"]}

    assert (
        links_by_rel["self"]
        == "https://example.com/stac/v1/collections/sentinel-2/items/test-item-123"
    )
    assert (
        links_by_rel["parent"] == "https://example.com/stac/v1/collections/sentinel-2"
    )
    assert (
        links_by_rel["collection"]
        == "https://example.com/stac/v1/collections/sentinel-2"
    )
    assert links_by_rel["root"] == "https://example.com/stac/v1"


def test_inject_stac_links_preserves_item_data():
    """Verify link injection doesn't remove other item properties."""
    add_links = inject_stac_links("http://localhost/stac")

    item = {
        "id": "item-1",
        "collection": "test-col",
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
        "assets": {"visual": {"href": "s3://bucket/image.tif"}},
    }

    result = add_links(item)

    # Verify original data is preserved
    assert result["id"] == "item-1"
    assert result["collection"] == "test-col"
    assert result["properties"]["datetime"] == "2024-01-01T00:00:00Z"
    assert "visual" in result["assets"]
    # And links were added
    assert "links" in result
