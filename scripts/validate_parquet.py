#!/usr/bin/env python3
"""
Validate GeoParquet files exported from pgSTAC.

This script connects to an S3-compatible storage (MinIO) and validates
that the exported GeoParquet files contain the expected STAC data.

Note: This validator supports two STAC GeoParquet schema formats:
  1. Nested properties (older): STAC properties stored in a 'properties' column
  2. Flattened properties (newer): STAC properties expanded as top-level columns

The stac-geoparquet library (used by pgstac_to_parquet) flattens properties
to top-level columns by default, which is the recommended format per the
STAC GeoParquet specification for better query performance.
"""

import sys
import argparse
from typing import Optional

try:
    import pyarrow.parquet as pq
    import s3fs
except ImportError:
    print("ERROR: Required dependencies not installed")
    print("Please install: pip install pyarrow s3fs")
    sys.exit(1)


def validate_parquet_files(
    bucket_path: str,
    access_key: str = "minioadmin",
    secret_key: str = "minioadmin",
    endpoint_url: str = "http://localhost:9000",
    expected_rows: Optional[int] = None,
) -> bool:
    """
    Validate GeoParquet files in S3 bucket.

    Args:
        bucket_path: S3 path to validate (e.g., 'test/geoparquet/test-collection')
        access_key: S3 access key
        secret_key: S3 secret key
        endpoint_url: S3 endpoint URL
        expected_rows: Expected number of rows (None to skip check)

    Returns:
        True if validation passes, False otherwise
    """
    # Setup S3 filesystem
    fs = s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        client_kwargs={"endpoint_url": endpoint_url},
        use_ssl=False,
    )

    try:
        # List files in bucket
        files = fs.ls(bucket_path)
        print(f"Found {len(files)} files in s3://{bucket_path}/")

        if not files:
            print(f"ERROR: No files found in s3://{bucket_path}/")
            print("\nAttempting to list parent path:")
            try:
                parent_path = "/".join(bucket_path.split("/")[:-1])
                print(f"  {fs.ls(parent_path)}")
            except Exception as e:
                print(f"  Could not list parent path: {e}")
            return False

        # Validate each parquet file
        parquet_files = [f for f in files if f.endswith(".parquet")]

        if not parquet_files:
            print(f"WARNING: No .parquet files found among {len(files)} files")
            return False

        for file_path in parquet_files:
            if not validate_single_file(fs, file_path, expected_rows):
                return False

        print(f"\n✓ All {len(parquet_files)} GeoParquet files validated successfully")
        return True

    except Exception as e:
        print(f"ERROR during validation: {e}")
        import traceback
        traceback.print_exc()
        return False


def validate_single_file(
    fs: s3fs.S3FileSystem,
    file_path: str,
    expected_rows: Optional[int] = None,
) -> bool:
    """
    Validate a single GeoParquet file.

    Args:
        fs: S3 filesystem instance
        file_path: Path to the parquet file
        expected_rows: Expected number of rows (None to skip check)

    Returns:
        True if validation passes, False otherwise
    """
    print(f"\nValidating: {file_path}")

    # Read parquet file
    with fs.open(file_path, "rb") as f:
        table = pq.read_table(f)

    print(f"  Rows: {table.num_rows}")
    print(f"  Columns: {table.num_columns}")
    print(f"  Column names: {table.column_names}")

    # Detect schema format
    has_properties_column = "properties" in table.column_names
    if has_properties_column:
        print(f"  Schema: Nested properties (older format)")
    else:
        print(f"  Schema: Flattened properties (newer format)")

    # Validate row count if specified
    if expected_rows is not None:
        if table.num_rows < expected_rows:
            print(f"ERROR: Expected at least {expected_rows} rows, found {table.num_rows}")
            return False
        elif table.num_rows > expected_rows:
            print(f"WARNING: Expected {expected_rows} rows, found {table.num_rows} (may include duplicates)")

    # Validate required STAC fields
    # Note: The 'properties' column is not strictly required in the newer format
    # where properties are flattened to top-level columns (e.g., 'datetime', 'created')
    required_fields = ["id", "geometry"]
    for field in required_fields:
        if field not in table.column_names:
            print(f"ERROR: Missing required field: {field}")
            return False

    # Verify we have either nested or flattened properties
    # - Nested: properties stored as a single struct column (older format)
    # - Flattened: properties expanded as individual columns (newer format)
    has_properties_column = "properties" in table.column_names
    has_flattened_properties = any(
        col in table.column_names for col in ["datetime", "created", "updated"]
    )

    if not has_properties_column and not has_flattened_properties:
        print("  WARNING: No properties column or common property fields found")
        print("    This may indicate an issue with the export schema")

    # Check geometry column
    if "geometry" in table.column_names:
        geom_col = table.column("geometry")
        null_count = geom_col.null_count
        if null_count > 0:
            print(f"WARNING: Found {null_count} null geometries")

    print("  ✓ Validation passed")
    return True


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Validate GeoParquet files from pgSTAC export"
    )
    parser.add_argument(
        "bucket_path",
        help="S3 bucket path to validate (e.g., test/geoparquet/test-collection)",
    )
    parser.add_argument(
        "--access-key",
        default="minioadmin",
        help="S3 access key (default: minioadmin)",
    )
    parser.add_argument(
        "--secret-key",
        default="minioadmin",
        help="S3 secret key (default: minioadmin)",
    )
    parser.add_argument(
        "--endpoint",
        default="http://localhost:9000",
        help="S3 endpoint URL (default: http://localhost:9000)",
    )
    parser.add_argument(
        "--expected-rows",
        type=int,
        help="Expected number of rows in parquet files",
    )

    args = parser.parse_args()

    success = validate_parquet_files(
        bucket_path=args.bucket_path,
        access_key=args.access_key,
        secret_key=args.secret_key,
        endpoint_url=args.endpoint,
        expected_rows=args.expected_rows,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
