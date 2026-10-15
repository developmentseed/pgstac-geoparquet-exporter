#!/usr/bin/env python3
"""STAC GeoParquet Exporter"""

import logging
import os
import sys
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

import psycopg
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import yaml
from stac_geoparquet.pgstac_reader import get_pgstac_partitions

from pgstac_geoparquet_exporter.writer import export_items_to_parquet

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


@dataclass
class ExportStats:
    total_collections_seen: int = 0
    skipped_empty: int = 0
    skipped_existing: int = 0
    exported: list[str] = field(default_factory=list)
    failed: dict[str, str] = field(default_factory=dict)


def inject_stac_links(stac_api_url: str) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """Returns a function that adds STAC API links to items."""

    def add_links(item: dict[str, Any]) -> dict[str, Any]:
        collection = item["collection"]
        item_id = item["id"]
        item["links"] = [
            {
                "rel": "self",
                "href": f"{stac_api_url}/collections/{collection}/items/{item_id}",
            },
            {"rel": "parent", "href": f"{stac_api_url}/collections/{collection}"},
            {"rel": "collection", "href": f"{stac_api_url}/collections/{collection}"},
            {"rel": "root", "href": stac_api_url},
        ]
        return item

    return add_links


def get_all_collections(conninfo: str) -> list[dict[str, Any]]:
    """Fetch all collection IDs from pgSTAC database."""
    conn = psycopg.connect(conninfo)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM pgstac.collections")
            collection_ids = [row[0] for row in cur.fetchall()]
            return [{"name": coll_id} for coll_id in collection_ids]
    finally:
        conn.close()


def build_incremental_output_file(output_path: str, dataset_dirname: str) -> str:
    """Return unique incremental parquet file path in dataset directory."""
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_id = uuid.uuid4().hex[:8]
    filename = f"{timestamp}_{run_id}.parquet"
    base_path = output_path.rstrip("/")
    normalized_dirname = dataset_dirname.strip("/") or "updates"
    return f"{base_path}/{normalized_dirname}/{filename}"


def output_exists(path: str, filesystem: Any | None) -> bool:
    """Check whether a readable Parquet file exists locally or remotely at path.

    Empty or truncated files (e.g. left by a failed run) count as missing.
    """
    if filesystem is None:
        if not Path(path).exists():
            return False
    elif filesystem.get_file_info(path).type == pafs.FileType.NotFound:  # type: ignore[attr-defined]
        return False
    try:
        pq.read_metadata(path, filesystem=filesystem)  # type: ignore[no-untyped-call]
    except (OSError, pa.ArrowException) as e:
        logger.warning(f"Existing {path} is not a readable Parquet file ({e})")
        return False
    return True


def export_options(coll: dict[str, Any]) -> dict[str, Any]:
    """Chunking and query settings set for this collection (writer has defaults)."""
    keys = ("chunk_size", "max_chunk_mb", "statement_timeout")
    return {key: coll[key] for key in keys if key in coll}


def parse_abfs_url(url: str) -> tuple[str, str]:
    """Return (account_name, container/path) for an abfs[s]:// URL.

    Expects abfs://<container>@<account>.dfs.core.windows.net/<path>.
    """
    parsed = urlparse(url)
    if parsed.scheme not in ("abfs", "abfss"):
        raise ValueError(f"Not an abfs:// URL: {url}")

    container, _, host = parsed.netloc.partition("@")
    account_name = host.split(".", 1)[0]
    if not container or not account_name:
        raise ValueError(
            f"Cannot determine Azure storage account/container from {url}; use "
            "abfs://<container>@<account>.dfs.core.windows.net/<path>"
        )

    path = parsed.path.strip("/")
    return account_name, f"{container}/{path}" if path else container


def export_partitions(
    conninfo: str,
    collection_id: str,
    output_dir: str,
    updated_after: datetime | None,
    options: dict[str, Any],
    row_func: Callable[[dict[str, Any]], dict[str, Any]],
    filesystem: Any | None,
    file_prefix: str = "",
) -> int:
    """Export one file per pgSTAC partition window of one collection."""
    exported = 0

    for partition in get_pgstac_partitions(conninfo, updated_after):
        if partition.collection != collection_id:
            continue
        partition_name = (
            Path(partition.partition).name or f"partition-{exported}.parquet"
        )
        output_file = f"{output_dir}/{file_prefix}{partition_name}"
        export_items_to_parquet(
            conninfo=conninfo,
            output_path=output_file,
            collection=collection_id,
            start_datetime=partition.start,
            end_datetime=partition.end,
            row_func=row_func,
            **options,
            filesystem=filesystem,
        )
        exported += 1
    return exported


def main() -> int:
    mode = os.environ.get("EXPORT_MODE", "complete")
    config_path = os.environ.get("CONFIG_PATH", "/config/export-config.yaml")
    output_base = os.environ.get("OUTPUT_PATH", "/output")
    stac_api_url = os.environ.get("STAC_API_URL")

    if not stac_api_url:
        print("ERROR: STAC_API_URL environment variable is required", file=sys.stderr)
        return 1

    row_func = inject_stac_links(stac_api_url)

    # Build PostgreSQL connection string
    conninfo = (
        f"host={os.environ['PGHOST']} "
        f"port={os.environ.get('PGPORT', '5432')} "
        f"dbname={os.environ['PGDATABASE']} "
        f"user={os.environ['PGUSER']} "
        f"password={os.environ['PGPASSWORD']}"
    )

    # Load config
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Determine which collections to export
    export_all = config.get("exportAll", False)
    if export_all:
        print("Fetching all collections from database...")
        collections = get_all_collections(conninfo)
        print(f"Found {len(collections)} collections")
    else:
        collections = config.get("collections") or []

    # Precedence: settings < overrides[collection] < collection entry
    settings = config.get("settings") or {}
    overrides = config.get("overrides") or {}
    collections = [
        {**settings, **overrides.get(coll["name"], {}), **coll} for coll in collections
    ]

    # Configure remote filesystem for S3-compatible or Azure Blob Storage
    filesystem = None
    if output_base.startswith("s3://"):
        endpoint = os.environ.get("AWS_ENDPOINT_URL")
        region = os.environ.get("AWS_REGION", "us-east-1")

        # endpoint_override supports S3-compatible servers
        filesystem = pafs.S3FileSystem(  # type: ignore[attr-defined]
            access_key=os.environ.get("AWS_ACCESS_KEY_ID"),
            secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            endpoint_override=endpoint,
            region=region,
            scheme="http" if endpoint and endpoint.startswith("http://") else "https",
            allow_bucket_creation=True,  # Only affects create_dir, not file writes
            allow_bucket_deletion=False,
        )
        # Strip s3:// prefix from output_base when using custom filesystem
        output_base = output_base[5:]  # Remove 's3://'
    elif output_base.startswith(("abfs://", "abfss://")):
        # Authenticates via DefaultAzureCredential (e.g. workload identity)
        account_name, output_base = parse_abfs_url(output_base)
        filesystem = pafs.AzureFileSystem(account_name=account_name)  # type: ignore[attr-defined]

    stats = ExportStats()

    if mode == "complete":
        # Complete export
        for coll in collections:
            collection_id = coll["name"]
            partition_frequency = coll.get("partition_frequency")
            stats.total_collections_seen += 1

            logger.info(f"Exporting collection: {collection_id}")

            # Determine output path
            output_path = f"{output_base}/{collection_id}"

            # Only create local directories if not using S3
            if filesystem is None:
                Path(output_path).mkdir(parents=True, exist_ok=True)

            try:
                if partition_frequency:
                    # One file per pgSTAC partition (datetime-based)
                    logger.info(
                        f"Using built-in pgstac partitioning for {collection_id}"
                    )
                    if not export_partitions(
                        conninfo=conninfo,
                        collection_id=collection_id,
                        # Same layout as stac-geoparquet's sync_pgstac_to_parquet
                        output_dir=f"{output_path}/{collection_id}",
                        updated_after=None,  # Export all partitions
                        options=export_options(coll),
                        row_func=row_func,
                        filesystem=filesystem,
                    ):
                        raise StopIteration
                else:
                    # Single file export - write to configurable filename (default: full.parquet)
                    filename = coll.get("complete_filename", "full.parquet")
                    output_file = f"{output_path}/{filename}"

                    # A complete snapshot is rewritten unless rewrite=False
                    if not coll.get("rewrite", True) and output_exists(
                        output_file, filesystem
                    ):
                        logger.info(
                            f"Skipping {collection_id} - file exists and rewrite=False"
                        )
                        stats.skipped_existing += 1
                        continue

                    export_items_to_parquet(
                        conninfo=conninfo,
                        output_path=output_file,
                        collection=collection_id,
                        row_func=row_func,
                        filesystem=filesystem,
                        **export_options(coll),
                    )

                    logger.info(f"Exported {collection_id} to {output_file}")
                stats.exported.append(collection_id)
            except StopIteration:
                logger.warning(
                    f"Skipping collection {collection_id}: no exportable items"
                )
                stats.skipped_empty += 1
            except Exception as e:
                error = f"{type(e).__name__}: {e}"
                logger.error(f"Failed to export collection {collection_id}: {error}")
                stats.failed[collection_id] = error
            finally:
                # Hand memory back before the next collection
                pa.default_memory_pool().release_unused()

    elif mode == "incremental":
        # Incremental mode - export only updated items to a delta parquet file.
        logger.info("Using incremental mode")

        for coll in collections:
            collection_id = coll["name"]
            output_path = f"{output_base}/{collection_id}"
            stats.total_collections_seen += 1

            # Only create local directories if not using S3
            if filesystem is None:
                Path(output_path).mkdir(parents=True, exist_ok=True)

            # Get last update timestamp if available
            updated_after = coll.get("updated_after")  # Should be datetime or None
            # The Helm chart renders timestamps as quoted strings
            if isinstance(updated_after, str):
                updated_after = datetime.fromisoformat(updated_after)

            # Write incremental updates as per-run files in a dataset directory.
            # Default layout: {collection}/updates/{timestamp}_{runid}.parquet
            dataset_dirname = coll.get("incremental_dirname", "updates")
            normalized_dirname = dataset_dirname.strip("/") or "updates"
            if filesystem is None:
                Path(f"{output_path}/{normalized_dirname}").mkdir(
                    parents=True, exist_ok=True
                )
            try:
                logger.info(
                    f"Exporting incremental updates for collection: {collection_id}"
                )
                if updated_after is None:
                    output_file = build_incremental_output_file(
                        output_path, normalized_dirname
                    )
                    export_items_to_parquet(
                        conninfo=conninfo,
                        output_path=output_file,
                        collection=collection_id,
                        row_func=row_func,
                        filesystem=filesystem,
                        **export_options(coll),
                    )
                    logger.info(
                        f"Exported incremental updates for {collection_id} to {output_file}"
                    )
                else:
                    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
                    exported_count = export_partitions(
                        conninfo=conninfo,
                        collection_id=collection_id,
                        output_dir=f"{output_path}/{normalized_dirname}",
                        updated_after=updated_after,
                        options=export_options(coll),
                        row_func=row_func,
                        filesystem=filesystem,
                        file_prefix=f"{timestamp}_{uuid.uuid4().hex[:8]}_",
                    )
                    if exported_count == 0:
                        raise StopIteration
                    logger.info(
                        f"Exported {exported_count} incremental partition file(s) for {collection_id}"
                    )
                stats.exported.append(collection_id)
            except StopIteration:
                logger.warning(f"Skipping collection {collection_id}: no updated items")
                stats.skipped_empty += 1
            except Exception as e:
                error = f"{type(e).__name__}: {e}"
                logger.error(f"Failed to export collection {collection_id}: {error}")
                stats.failed[collection_id] = error
            finally:
                # Hand memory back before the next collection
                pa.default_memory_pool().release_unused()
    else:
        logger.error(f"Unknown export mode: {mode}")
        return 1

    # Print summary
    logger.info("\n=== Export Summary ===")
    logger.info(f"Total collections seen: {stats.total_collections_seen}")
    logger.info(f"Exported successfully: {len(stats.exported)}")
    logger.info(f"Skipped (empty): {stats.skipped_empty}")
    logger.info(f"Skipped (existing): {stats.skipped_existing}")
    logger.info(f"Failed: {len(stats.failed)}")
    if stats.exported:
        logger.info(f"Exported collections: {', '.join(stats.exported)}")
    for collection_id, error in stats.failed.items():
        logger.error(f"Failed collection {collection_id}: {error}")

    if stats.failed:
        logger.error("Export completed with errors")
        return 1
    else:
        logger.info("Export complete")
        return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)
