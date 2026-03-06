#!/usr/bin/env python3
"""STAC GeoParquet Exporter"""

import os
import sys
from pathlib import Path

import yaml
from stac_geoparquet.pgstac_reader import CollectionConfig


def main() -> int:
    mode = os.environ.get("EXPORT_MODE", "complete")
    config_path = os.environ.get("CONFIG_PATH", "/config/export-config.yaml")
    output_base = os.environ.get("OUTPUT_PATH", "/output")
    stac_api = os.environ.get(
        "STAC_API_URL", "https://planetarycomputer.microsoft.com/api/stac/v1"
    )

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

    if mode == "complete":
        # Complete export with optional partitioning
        for coll in config.get("collections", []):
            collection_id = coll["name"]
            partition_frequency = coll.get(
                "partition_frequency"
            )  # e.g., "YS" for yearly, "MS" for monthly

            print(f"Exporting collection: {collection_id}")

            # Create CollectionConfig
            collection_config = CollectionConfig(
                collection_id=collection_id,
                partition_frequency=partition_frequency,
                stac_api=stac_api,
                should_inject_dynamic_properties=coll.get(
                    "inject_dynamic_properties", False
                ),
            )

            # Determine output path
            output_path = f"{output_base}/{collection_id}"
            Path(output_path).mkdir(parents=True, exist_ok=True)

            if not partition_frequency:
                # Single file export
                output_file = f"{output_path}/items.parquet"
            else:
                # Partitioned export - output_path is the directory
                output_file = output_path

            # Export the collection
            results = collection_config.export_collection(
                conninfo=conninfo,
                output_protocol="file",
                output_path=output_file,
                storage_options={},
                rewrite=coll.get("rewrite", False),
                skip_empty_partitions=coll.get("skip_empty_partitions", True),
            )

            exported_count = len([r for r in results if r is not None])
            print(f"Exported {exported_count} partition(s) for {collection_id}")

    else:
        # Incremental mode - export only updated items
        print("Incremental mode not fully supported by stac-geoparquet API")
        print("Consider using complete mode with rewrite=false to skip existing files")
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
