# pgSTAC GeoParquet Exporter

Export STAC collections from pgSTAC to GeoParquet format with support for complete and incremental exports.

## Features

- **Complete Export**: Full export of collections with optional yearly partitioning
- **Incremental Export**: Sync only changed items since last run
- **Configuration-based**: YAML config for collections and partitioning strategy

## Installation

```bash
pip install -e .
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
```

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

**Complete**: Exports entire collections. With yearly partitioning, creates separate files per year.

**Incremental**: Tracks last sync time and exports only updated items. State stored in `{OUTPUT_PATH}/.last_sync`.
