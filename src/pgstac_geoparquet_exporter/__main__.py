#!/usr/bin/env python3
"""STAC GeoParquet Exporter"""

import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional, Union

import psycopg
import pyarrow.fs as pafs
import yaml
from stac_geoparquet.pgstac_reader import (
    get_pgstac_partitions,
    pgstac_to_parquet,
    sync_pgstac_to_parquet,
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


@dataclass
class ExportStats:
    total_collections_seen: int = 0
    exported_successfully: int = 0
    skipped_empty: int = 0
    skipped_existing: int = 0
    failed_collections: int = 0


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


def sync_collection_to_parquet(
    conninfo: str,
    collection: str,
    output_path: Union[str, Path],
    updated_after: Union[datetime, None] = None,
    chunk_size: int = 8192,
    row_func: Union[Callable[[Any], Any], None] = None,
    filesystem: Optional[Any] = None,
    **kwargs: Any,
) -> None:
    """
    Sync a single collection to parquet, filtering partitions by collection.

    This is a wrapper around sync_pgstac_to_parquet that ensures only partitions
    for the specified collection are exported. This prevents cross-collection
    contamination in incremental mode.

    Args:
        conninfo: PostgreSQL connection string
        collection: Collection ID to sync
        output_path: Base output path (collection will NOT be added as subdirectory)
        updated_after: Only sync partitions updated after this timestamp
        chunk_size: Number of items to process per chunk
        row_func: Optional function to transform each row
        filesystem: Optional filesystem to use for output
        **kwargs: Additional arguments passed to pgstac_to_parquet
    """
    if filesystem is None:
        filesystem_obj, filepath = pafs.FileSystem.from_uri(str(output_path))  # type: ignore[attr-defined]
    else:
        filesystem_obj = filesystem
        filepath = str(output_path)

    filedir = Path(filepath)
    filesystem_obj.create_dir(str(filedir), recursive=True)

    logger.debug(
        f"Syncing collection {collection} to {output_path} (updated_after={updated_after})"
    )

    # Get all partitions and filter by collection
    partitions_found = 0
    for partition in get_pgstac_partitions(conninfo, updated_after):
        if partition.collection != collection:
            # Skip partitions from other collections
            logger.debug(
                f"Skipping partition for collection {partition.collection} "
                f"(looking for {collection})"
            )
            continue

        partitions_found += 1
        output_file = filedir / partition.partition
        logger.debug(
            f"Syncing partition {partition.partition} for collection {collection} "
            f"to {output_file}"
        )

        pgstac_to_parquet(
            conninfo=conninfo,
            output_path=str(output_file),
            collection=partition.collection,
            start_datetime=partition.start,
            end_datetime=partition.end,
            chunk_size=chunk_size,
            row_func=row_func,
            filesystem=filesystem,
            **kwargs,
        )

    if partitions_found == 0:
        logger.info(
            f"No partitions found for collection {collection} "
            f"(updated_after={updated_after})"
        )


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
        # Apply default settings from config
        default_settings = config.get("exportConfig", {}).get("settings", {})
        for coll in collections:
            coll.update(default_settings)
    else:
        collections = config.get("collections", [])

    # Configure S3 filesystem if using S3/MinIO
    filesystem = None
    if output_base.startswith("s3://"):
        endpoint = os.environ.get("AWS_ENDPOINT_URL")
        region = os.environ.get("AWS_REGION", "us-east-1")

        # Create S3FileSystem with MinIO-compatible options
        filesystem = pafs.S3FileSystem(  # type: ignore[attr-defined]
            access_key=os.environ.get("AWS_ACCESS_KEY_ID"),
            secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            endpoint_override=endpoint,
            region=region,
            scheme="http" if endpoint and endpoint.startswith("http://") else "https",
            allow_bucket_creation=True,  # Required for MinIO
            allow_bucket_deletion=False,
        )
        # Strip s3:// prefix from output_base when using custom filesystem
        output_base = output_base[5:]  # Remove 's3://'

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
                    # Use sync_pgstac_to_parquet for partitioned exports
                    # This uses pgstac's built-in partitioning based on datetime
                    logger.info(
                        f"Using built-in pgstac partitioning for {collection_id}"
                    )
                    sync_pgstac_to_parquet(
                        conninfo=conninfo,
                        output_path=output_path,
                        updated_after=None,  # Export all partitions
                        chunk_size=coll.get("chunk_size", 8192),
                        row_func=row_func,
                        filesystem=filesystem,
                    )
                else:
                    # Single file export
                    output_file = f"{output_path}/items.parquet"

                    # Check if file exists and skip if rewrite=False
                    if Path(output_file).exists() and not coll.get("rewrite", False):
                        logger.info(
                            f"Skipping {collection_id} - file exists and rewrite=False"
                        )
                        stats.skipped_existing += 1
                        continue

                    pgstac_to_parquet(
                        conninfo=conninfo,
                        output_path=output_file,
                        collection=collection_id,
                        chunk_size=coll.get("chunk_size", 8192),
                        row_func=row_func,
                        filesystem=filesystem,
                    )

                    logger.info(f"Exported {collection_id} to {output_file}")
                stats.exported_successfully += 1
            except StopIteration:
                logger.warning(
                    f"Skipping collection {collection_id}: no exportable items"
                )
                stats.skipped_empty += 1
            except Exception as e:
                logger.error(f"Failed to export collection {collection_id}: {e}")
                stats.failed_collections += 1

    elif mode == "incremental":
        # Incremental mode - export only updated items
        logger.info("Using incremental mode with collection-scoped sync")

        for coll in collections:
            collection_id = coll["name"]
            output_path = f"{output_base}/{collection_id}"
            stats.total_collections_seen += 1

            # Only create local directories if not using S3
            if filesystem is None:
                Path(output_path).mkdir(parents=True, exist_ok=True)

            # Get last update timestamp if available
            updated_after = coll.get("updated_after")  # Should be datetime or None

            try:
                logger.info(f"Syncing collection: {collection_id}")
                sync_collection_to_parquet(
                    conninfo=conninfo,
                    collection=collection_id,
                    output_path=output_path,
                    updated_after=updated_after,
                    chunk_size=coll.get("chunk_size", 8192),
                    row_func=row_func,
                    filesystem=filesystem,
                )
                logger.info(f"Synced {collection_id}")
                stats.exported_successfully += 1
            except StopIteration:
                logger.warning(
                    f"Skipping collection {collection_id}: no exportable items"
                )
                stats.skipped_empty += 1
            except Exception as e:
                logger.error(f"Failed to export collection {collection_id}: {e}")
                stats.failed_collections += 1
    else:
        logger.error(f"Unknown export mode: {mode}")
        return 1

    # Print summary
    logger.info("\n=== Export Summary ===")
    logger.info(f"Total collections seen: {stats.total_collections_seen}")
    logger.info(f"Exported successfully: {stats.exported_successfully}")
    logger.info(f"Skipped (empty): {stats.skipped_empty}")
    logger.info(f"Skipped (existing): {stats.skipped_existing}")
    logger.info(f"Failed: {stats.failed_collections}")

    if stats.failed_collections > 0:
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
