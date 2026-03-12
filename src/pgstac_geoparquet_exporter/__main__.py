#!/usr/bin/env python3
"""STAC GeoParquet Exporter"""

import os
import sys
from pathlib import Path
from typing import Any, Callable

import pyarrow.fs as pafs  # type: ignore
import yaml
from stac_geoparquet.pgstac_reader import pgstac_to_parquet, sync_pgstac_to_parquet


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

    # Configure S3 filesystem if using S3/MinIO
    filesystem = None
    if output_base.startswith("s3://"):
        endpoint = os.environ.get("AWS_ENDPOINT_URL")
        region = os.environ.get("AWS_REGION", "us-east-1")

        # Create S3FileSystem with MinIO-compatible options
        filesystem = pafs.S3FileSystem(
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

    if mode == "complete":
        # Complete export
        for coll in config.get("collections", []):
            collection_id = coll["name"]
            partition_frequency = coll.get("partition_frequency")

            print(f"Exporting collection: {collection_id}")

            # Determine output path
            output_path = f"{output_base}/{collection_id}"

            # Only create local directories if not using S3
            if filesystem is None:
                Path(output_path).mkdir(parents=True, exist_ok=True)

            if partition_frequency:
                # Use sync_pgstac_to_parquet for partitioned exports
                # This uses pgstac's built-in partitioning based on datetime
                print(f"Using built-in pgstac partitioning for {collection_id}")
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
                    print(f"Skipping {collection_id} - file exists and rewrite=False")
                    continue

                pgstac_to_parquet(
                    conninfo=conninfo,
                    output_path=output_file,
                    collection=collection_id,
                    chunk_size=coll.get("chunk_size", 8192),
                    row_func=row_func,
                    filesystem=filesystem,
                )

                print(f"Exported {collection_id} to {output_file}")

    elif mode == "incremental":
        # Incremental mode - export only updated items
        print("Using incremental mode with sync_pgstac_to_parquet")

        for coll in config.get("collections", []):
            collection_id = coll["name"]
            output_path = f"{output_base}/{collection_id}"

            # Only create local directories if not using S3
            if filesystem is None:
                Path(output_path).mkdir(parents=True, exist_ok=True)

            # Get last update timestamp if available
            updated_after = coll.get("updated_after")  # Should be datetime or None

            print(f"Syncing collection: {collection_id}")
            sync_pgstac_to_parquet(
                conninfo=conninfo,
                output_path=output_path,
                updated_after=updated_after,
                chunk_size=coll.get("chunk_size", 8192),
                row_func=row_func,
                filesystem=filesystem,
            )
            print(f"Synced {collection_id}")
    else:
        print(f"Unknown export mode: {mode}", file=sys.stderr)
        return 1

    print("Export complete")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)
