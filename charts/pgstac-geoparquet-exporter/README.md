# pgSTAC GeoParquet Exporter Helm Chart

Export STAC collections from pgSTAC to GeoParquet with automated CronJobs.

## Installation

```bash
# Create secrets
kubectl create secret generic pgstac-db \
  --from-literal=host=postgres.example.com \
  --from-literal=port=5432 \
  --from-literal=dbname=pgstac \
  --from-literal=user=postgres \
  --from-literal=password=secret \
  -n data-access

kubectl create secret generic s3-creds \
  --from-literal=AWS_ACCESS_KEY_ID=key \
  --from-literal=AWS_SECRET_ACCESS_KEY=secret \
  -n data-access

# Install
helm install exporter ./charts/pgstac-geoparquet-exporter \
  --namespace data-access \
  --create-namespace \
  --set database.existingSecret=pgstac-db \
  --set storage.existingSecret=s3-creds \
  --set storage.outputPath=s3://my-bucket/exports
```

## Configuration

Edit `values.yaml` or create a custom values file:

```yaml
# Collections to export
exportConfig:
  collections:
    - name: sentinel-2
      partition_by: year
      start_year: 2015
    - name: landsat-8
      partition_by: null  # Single file

# Schedules
completeExport:
  schedule: "0 2 1 * *"  # Monthly
incrementalExport:
  schedule: "0 3 * * *"  # Daily

# Resources
incrementalExport:
  resources:
    requests:
      memory: "1Gi"
      cpu: "250m"
```

## Key Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `database.existingSecret` | DB credentials secret | `default-pguser-eoapi` |
| `storage.outputPath` | Output path (s3:// or local) | `s3://eoapi-geoparquet/geoparquet` |
| `storage.existingSecret` | S3 credentials secret | `data-access` |
| `completeExport.schedule` | Complete export cron | `"0 2 1 * *"` |
| `incrementalExport.schedule` | Incremental export cron | `"0 3 * * *"` |

## Usage

```bash
# Manual trigger
kubectl create job --from=cronjob/exporter-incremental test-$(date +%s) -n data-access

# View logs
kubectl logs -n data-access -l app.kubernetes.io/name=pgstac-geoparquet-exporter -f

# Update config
helm upgrade exporter ./charts/pgstac-geoparquet-exporter -n data-access -f values.yaml
```

## Export Modes

- **Complete**: Full export with optional yearly/monthly partitioning
- **Incremental**: Only changed items since last run (state in `{OUTPUT_PATH}/.last_sync`)
