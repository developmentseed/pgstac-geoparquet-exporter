# pgSTAC GeoParquet Exporter

Export STAC collections from pgSTAC to GeoParquet format with support for complete and incremental exports.

## Features

- **Complete Export**: Full export of collections with optional yearly partitioning
- **Incremental Export**: Sync only changed items since last run
- **Configuration-based**: YAML config for collections and partitioning strategy

## Installation

```bash
uv pip install -e .
```

## Usage

Set required environment variables:

```bash
export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=pgstac
export PGUSER=postgres
export PGPASSWORD=secret
export CONFIG_PATH=/path/to/export-config.yaml
export OUTPUT_PATH=/output
export EXPORT_MODE=incremental  # or "complete"
```

Run the exporter:

```bash
python -m pgstac_geoparquet_exporter
```

## Configuration

Create `export-config.yaml`:

```yaml
collections:
  - name: sentinel-2
    partition_by: year
    start_year: 2015
  - name: landsat
    partition_by: null  # Single file
    complete_filename: baseline.parquet  # Optional, defaults to "full.parquet"
    incremental_dirname: delta  # Optional, defaults to "updates" (directory)
```

### Configuration Options

Per-collection settings:

- `name`: Collection ID (required)
- `partition_frequency`: Partition by time (`YS` for yearly, `MS` for monthly, etc.)
- `chunk_size`: Number of items per chunk (default: 8192)
- `rewrite`: Whether to overwrite existing files (default: false)
- `updated_after`: Datetime filter for incremental exports
- `complete_filename`: Filename for non-partitioned complete exports (default: `full.parquet`)
- `incremental_dirname`: Directory name for incremental run files (default: `updates`)

## Docker

Build:

```bash
docker build -t pgstac-geoparquet-exporter .
```

Run:

```bash
docker run --rm \
  -e PGHOST=localhost \
  -e PGDATABASE=pgstac \
  -e PGUSER=postgres \
  -e PGPASSWORD=secret \
  -e EXPORT_MODE=incremental \
  -v $(pwd)/config:/config \
  -v $(pwd)/output:/output \
  pgstac-geoparquet-exporter
```

## Export Modes

### Complete Mode

Exports entire collections to `{collection-name}/full.parquet`. This creates a baseline snapshot of all items in the collection.

- For collections without partitioning: Single `full.parquet` file
- For collections with yearly partitioning: Separate files per year (partition files)

### Incremental Mode

Exports only changed items since the last update to `{collection-name}/updates/` (or your configured `incremental_dirname` directory). Each run writes new parquet output:

- Default (no `updated_after`): `{timestamp}_{runid}.parquet`
- With `updated_after`: one file per matching partition window, `{timestamp}_{runid}_{partition}.parquet`
- Examples: `updates/20260505T142300Z_deadbeef.parquet`, `updates/20260505T142300Z_deadbeef_items_202401.parquet`

**Typical Workflow:**

1. Generate monthly baseline: `EXPORT_MODE=complete` → creates `{collection}/full.parquet`
2. Daily/weekly updates: `EXPORT_MODE=incremental` → writes new files in `{collection}/updates/`
3. Monthly refresh: Regenerate `full.parquet`, then archive or clear old files from `updates/`

**Custom Filenames Example:**

```yaml
collections:
  - name: sentinel-2
    complete_filename: baseline.parquet
    incremental_dirname: delta
```

This creates `sentinel-2/baseline.parquet` (complete) and per-run incremental files under `sentinel-2/delta/`.

**Note:** Use the `updated_after` parameter in your collection configuration to control which items are considered "updated".
