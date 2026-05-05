import os
import sys
from datetime import datetime, timezone
from unittest.mock import Mock, patch, mock_open, MagicMock

import pytest

# Mock heavy dependencies
sys.modules["stac_geoparquet"] = Mock()
sys.modules["stac_geoparquet.pgstac_reader"] = Mock()

from pgstac_geoparquet_exporter.__main__ import (  # noqa: E402
    inject_stac_links,
    main,
    sync_collection_to_parquet,
)


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


@patch("pgstac_geoparquet_exporter.__main__.sync_collection_to_parquet")
@patch("pathlib.Path.mkdir")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: test\n",
)
def test_incremental_mode_with_collection(mock_file, mock_mkdir, mock_sync, base_env):
    """Verify incremental mode calls sync_collection_to_parquet"""
    base_env["EXPORT_MODE"] = "incremental"

    with patch.dict(os.environ, base_env, clear=True):
        result = main()

    assert result == 0
    mock_sync.assert_called_once()
    call_kwargs = mock_sync.call_args[1]
    assert call_kwargs["collection"] == "test"
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


@patch("pgstac_geoparquet_exporter.__main__.pgstac_to_parquet")
@patch("pathlib.Path.mkdir")
@patch("pathlib.Path.exists")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: empty-col\n",
)
def test_empty_collection_does_not_crash(
    mock_file, mock_exists, mock_mkdir, mock_to_parquet, base_env
):
    """Verify empty collection raises StopIteration and is handled gracefully"""
    mock_exists.return_value = False
    mock_to_parquet.side_effect = StopIteration  # Empty collection
    base_env["EXPORT_MODE"] = "complete"

    with patch.dict(os.environ, base_env, clear=True):
        result = main()

    # Should exit 0 when only empty collections are skipped
    assert result == 0
    mock_to_parquet.assert_called_once()


@patch("pgstac_geoparquet_exporter.__main__.pgstac_to_parquet")
@patch("pathlib.Path.mkdir")
@patch("pathlib.Path.exists")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: valid-col\n  - name: empty-col\n",
)
def test_mixed_valid_and_empty_collections(
    mock_file, mock_exists, mock_mkdir, mock_to_parquet, base_env
):
    """Verify mixed case: one valid collection and one empty exports valid one and exits 0"""
    mock_exists.return_value = False
    # First call succeeds, second raises StopIteration
    mock_to_parquet.side_effect = [None, StopIteration]
    base_env["EXPORT_MODE"] = "complete"

    with patch.dict(os.environ, base_env, clear=True):
        result = main()

    # Should exit 0 when only empty collections are skipped
    assert result == 0
    assert mock_to_parquet.call_count == 2


@patch("pgstac_geoparquet_exporter.__main__.pgstac_to_parquet")
@patch("pathlib.Path.mkdir")
@patch("pathlib.Path.exists")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: error-col\n",
)
def test_real_error_causes_nonzero_exit(
    mock_file, mock_exists, mock_mkdir, mock_to_parquet, base_env
):
    """Verify real error (not StopIteration) causes non-zero exit"""
    mock_exists.return_value = False
    mock_to_parquet.side_effect = RuntimeError("Network error")
    base_env["EXPORT_MODE"] = "complete"

    with patch.dict(os.environ, base_env, clear=True):
        result = main()

    # Should exit 1 when real errors occur
    assert result == 1
    mock_to_parquet.assert_called_once()


@patch("pgstac_geoparquet_exporter.__main__.pgstac_to_parquet")
@patch("pathlib.Path.mkdir")
@patch("pathlib.Path.exists")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: col1\n  - name: col2\n  - name: col3\n",
)
def test_all_collections_fail_returns_nonzero(
    mock_file, mock_exists, mock_mkdir, mock_to_parquet, base_env, caplog
):
    """Verify exit code 1 when all collections fail"""
    caplog.set_level("INFO")
    mock_exists.return_value = False
    mock_to_parquet.side_effect = RuntimeError("DB connection failed")
    base_env["EXPORT_MODE"] = "complete"

    with patch.dict(os.environ, base_env, clear=True):
        result = main()

    assert result == 1
    assert mock_to_parquet.call_count == 3
    log_text = caplog.text
    assert "Failed: 3" in log_text
    assert "Exported successfully: 0" in log_text


@patch("pgstac_geoparquet_exporter.__main__.pgstac_to_parquet")
@patch("pathlib.Path.mkdir")
@patch("pathlib.Path.exists")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: col1\n  - name: col2\n  - name: col3\n",
)
def test_summary_stats_logged(
    mock_file, mock_exists, mock_mkdir, mock_to_parquet, base_env, caplog
):
    """Verify summary statistics are logged correctly"""
    caplog.set_level("INFO")
    mock_exists.return_value = False
    # col1: success, col2: empty, col3: error
    mock_to_parquet.side_effect = [None, StopIteration, RuntimeError("Test error")]
    base_env["EXPORT_MODE"] = "complete"

    with patch.dict(os.environ, base_env, clear=True):
        result = main()

    log_text = caplog.text
    # Check summary is present
    assert "Export Summary" in log_text
    assert "Total collections seen: 3" in log_text
    assert "Exported successfully: 1" in log_text
    assert "Skipped (empty): 1" in log_text
    assert "Failed: 1" in log_text
    # Should exit 1 because of the error
    assert result == 1


@patch("pgstac_geoparquet_exporter.__main__.sync_pgstac_to_parquet")
@patch("pathlib.Path.mkdir")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: empty-partitioned\n    partition_frequency: YS\n",
)
def test_empty_partitioned_collection_does_not_crash(
    mock_file, mock_mkdir, mock_sync, base_env
):
    """Verify empty partitioned collection is handled gracefully"""
    mock_sync.side_effect = StopIteration  # Empty collection
    base_env["EXPORT_MODE"] = "complete"

    with patch.dict(os.environ, base_env, clear=True):
        result = main()

    # Should exit 0 when only empty collections are skipped
    assert result == 0
    mock_sync.assert_called_once()


@patch("pgstac_geoparquet_exporter.__main__.sync_collection_to_parquet")
@patch("pathlib.Path.mkdir")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: empty-col\n",
)
def test_incremental_mode_empty_collection(mock_file, mock_mkdir, mock_sync, base_env):
    """Verify incremental mode handles empty collections gracefully"""
    mock_sync.side_effect = StopIteration
    base_env["EXPORT_MODE"] = "incremental"

    with patch.dict(os.environ, base_env, clear=True):
        result = main()

    # Should exit 0 when only empty collections are skipped
    assert result == 0
    mock_sync.assert_called_once()


@patch("pgstac_geoparquet_exporter.__main__.pgstac_to_parquet")
@patch("pathlib.Path.mkdir")
@patch("pathlib.Path.exists")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: col1\n  - name: col2\n    rewrite: false\n",
)
def test_skipped_file_not_counted_as_failure(
    mock_file, mock_exists, mock_mkdir, mock_to_parquet, base_env, caplog
):
    """Verify that skipped files (rewrite=false) are tracked separately"""
    caplog.set_level("INFO")

    # First call is col1 (doesn't exist), second is col2 (exists)
    mock_exists.side_effect = [False, True]
    base_env["EXPORT_MODE"] = "complete"

    with patch.dict(os.environ, base_env, clear=True):
        result = main()

    # col1 should be exported, col2 should skip due to file existing and rewrite=false
    assert mock_to_parquet.call_count == 1  # Only col1
    assert result == 0  # Should still exit 0

    log_text = caplog.text
    assert "Total collections seen: 2" in log_text
    assert "Exported successfully: 1" in log_text
    assert "Skipped (existing): 1" in log_text
    assert "Skipped (empty): 0" in log_text
    assert "Failed: 0" in log_text


# Tests for cross-collection bug fix


@patch("pgstac_geoparquet_exporter.__main__.get_pgstac_partitions")
@patch("pgstac_geoparquet_exporter.__main__.pgstac_to_parquet")
def test_sync_collection_filters_partitions_by_collection(
    mock_to_parquet, mock_get_partitions
):
    """Verify sync_collection_to_parquet only processes partitions for the requested collection"""
    from pgstac_geoparquet_exporter.__main__ import sync_collection_to_parquet

    # Create mock Partition objects
    mock_partition_a = MagicMock()
    mock_partition_a.collection = "collection-a"
    mock_partition_a.partition = "items.parquet"
    mock_partition_a.start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    mock_partition_a.end = datetime(2024, 12, 31, tzinfo=timezone.utc)

    mock_partition_b = MagicMock()
    mock_partition_b.collection = "collection-b"
    mock_partition_b.partition = "items.parquet"
    mock_partition_b.start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    mock_partition_b.end = datetime(2024, 12, 31, tzinfo=timezone.utc)

    # get_pgstac_partitions returns partitions from both collections
    mock_get_partitions.return_value = [mock_partition_a, mock_partition_b]

    # Create mock filesystem
    mock_filesystem = MagicMock()
    mock_filesystem.create_dir = MagicMock()

    # Sync only collection-a
    sync_collection_to_parquet(
        conninfo="host=test",
        collection="collection-a",
        output_path="/output/collection-a",
        filesystem=mock_filesystem,
    )

    # Verify only collection-a partition was exported
    assert mock_to_parquet.call_count == 1
    call_kwargs = mock_to_parquet.call_args[1]
    assert call_kwargs["collection"] == "collection-a"


@patch("pgstac_geoparquet_exporter.__main__.sync_collection_to_parquet")
@patch("pathlib.Path.mkdir")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: collection-a\n  - name: collection-b\n",
)
def test_incremental_mode_multiple_collections_isolated(
    mock_file, mock_mkdir, mock_sync, base_env
):
    """Verify incremental mode calls sync_collection_to_parquet separately for each collection"""
    base_env["EXPORT_MODE"] = "incremental"

    with patch.dict(os.environ, base_env, clear=True):
        result = main()

    assert result == 0
    assert mock_sync.call_count == 2

    # Verify each collection was synced with its own collection parameter
    calls = mock_sync.call_args_list
    call_collections = [call[1]["collection"] for call in calls]
    assert "collection-a" in call_collections
    assert "collection-b" in call_collections

    # Verify output paths are correct
    call_paths = [call[1]["output_path"] for call in calls]
    assert "/output/collection-a" in call_paths
    assert "/output/collection-b" in call_paths


@patch("pgstac_geoparquet_exporter.__main__.get_pgstac_partitions")
@patch("pgstac_geoparquet_exporter.__main__.pgstac_to_parquet")
def test_sync_collection_handles_empty_collection(mock_to_parquet, mock_get_partitions):
    """Verify sync_collection_to_parquet handles empty collections gracefully"""
    # No partitions for the requested collection
    mock_partition_other = MagicMock()
    mock_partition_other.collection = "other-collection"
    mock_get_partitions.return_value = [mock_partition_other]

    # Create mock filesystem
    mock_filesystem = MagicMock()
    mock_filesystem.create_dir = MagicMock()

    # Should complete without error
    sync_collection_to_parquet(
        conninfo="host=test",
        collection="empty-collection",
        output_path="/output/empty-collection",
        filesystem=mock_filesystem,
    )

    # Verify pgstac_to_parquet was never called
    assert mock_to_parquet.call_count == 0


@patch("pgstac_geoparquet_exporter.__main__.get_pgstac_partitions")
@patch("pgstac_geoparquet_exporter.__main__.pgstac_to_parquet")
def test_sync_collection_multiple_partitions_same_collection(
    mock_to_parquet, mock_get_partitions
):
    """Verify sync_collection_to_parquet handles multiple partitions from same collection"""
    # Create two partitions for collection-a
    mock_partition_1 = MagicMock()
    mock_partition_1.collection = "collection-a"
    mock_partition_1.partition = "items_20240101_20240630.parquet"
    mock_partition_1.start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    mock_partition_1.end = datetime(2024, 6, 30, tzinfo=timezone.utc)

    mock_partition_2 = MagicMock()
    mock_partition_2.collection = "collection-a"
    mock_partition_2.partition = "items_20240701_20241231.parquet"
    mock_partition_2.start = datetime(2024, 7, 1, tzinfo=timezone.utc)
    mock_partition_2.end = datetime(2024, 12, 31, tzinfo=timezone.utc)

    mock_get_partitions.return_value = [mock_partition_1, mock_partition_2]

    # Create mock filesystem
    mock_filesystem = MagicMock()
    mock_filesystem.create_dir = MagicMock()

    sync_collection_to_parquet(
        conninfo="host=test",
        collection="collection-a",
        output_path="/output/collection-a",
        filesystem=mock_filesystem,
    )

    # Verify both partitions were exported
    assert mock_to_parquet.call_count == 2

    # Verify both calls are for collection-a
    for call in mock_to_parquet.call_args_list:
        assert call[1]["collection"] == "collection-a"


@patch("pgstac_geoparquet_exporter.__main__.sync_collection_to_parquet")
@patch("pathlib.Path.mkdir")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="collections:\n  - name: noaa-emergency-response\n  - name: sentinel-2-iceland\n  - name: eric.test\n  - name: eric.test2\n",
)
def test_incremental_mode_regression_cross_collection_isolation(
    mock_file, mock_mkdir, mock_sync, base_env
):
    """
    Regression test for cross-collection contamination bug.

    Bug: When syncing collection X in incremental mode, files were written to
    .../geoparquet/X/Y/items.parquet where Y is a different collection.

    This test verifies that each collection is synced independently with proper
    collection filtering.
    """
    base_env["EXPORT_MODE"] = "incremental"

    with patch.dict(os.environ, base_env, clear=True):
        result = main()

    assert result == 0
    assert mock_sync.call_count == 4

    # Verify each sync call has matching collection and output path
    calls = mock_sync.call_args_list
    for call in calls:
        collection = call[1]["collection"]
        output_path = call[1]["output_path"]

        # Output path should end with the collection name
        assert output_path.endswith(
            collection
        ), f"Output path {output_path} does not match collection {collection}"

        # Verify collection parameter is passed
        assert "collection" in call[1]


@patch("pgstac_geoparquet_exporter.__main__.get_pgstac_partitions")
@patch("pgstac_geoparquet_exporter.__main__.pgstac_to_parquet")
def test_sync_collection_updated_after_filter(mock_to_parquet, mock_get_partitions):
    """Verify sync_collection_to_parquet respects updated_after parameter"""
    mock_partition = MagicMock()
    mock_partition.collection = "test-collection"
    mock_partition.partition = "items.parquet"
    mock_partition.start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    mock_partition.end = datetime(2024, 12, 31, tzinfo=timezone.utc)
    mock_get_partitions.return_value = [mock_partition]

    updated_after = datetime(2024, 6, 1, tzinfo=timezone.utc)

    # Create mock filesystem
    mock_filesystem = MagicMock()
    mock_filesystem.create_dir = MagicMock()

    sync_collection_to_parquet(
        conninfo="host=test",
        collection="test-collection",
        output_path="/output/test-collection",
        updated_after=updated_after,
        filesystem=mock_filesystem,
    )

    # Verify get_pgstac_partitions was called with updated_after
    mock_get_partitions.assert_called_once_with("host=test", updated_after)
