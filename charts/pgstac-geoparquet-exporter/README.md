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
  --set storage.outputPath=s3://my-bucket/exports \
  --set stacApiUrl=https://example.com/stac/v1
```

## Configuration

Edit `values.yaml` or create a custom values file:

```yaml
# STAC API URL (required for link injection)
stacApiUrl: "https://example.com/stac/v1"

# Collections to export
exportConfig:
  # Export all collections from database (ignores collections list below)
  exportAll: false
  
  # Specify individual collections (or leave empty and set exportAll: true)
  collections:
    - name: sentinel-2
      partition_frequency: YS  # One file per pgSTAC partition
    - name: landsat-8          # Single full.parquet

  # Defaults for every collection
  settings:
    chunk_size: 8192
    max_chunk_mb: 64
    statement_timeout: 3600000
    rewrite: true

  # Per-collection settings, also applied with exportAll: true
  overrides:
    emdat-events:
      chunk_size: 500

# Job schedules and resources
jobs:
  complete:
    schedule: "0 2 1 * *"  # Monthly
    resources:
      requests:
        memory: "2Gi"
        cpu: "500m"

  incremental:
    schedule: "0 3 * * *"  # Daily
    resources:
      requests:
        memory: "1Gi"
        cpu: "250m"
```

## Key Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `stacApiUrl` | **Required**. STAC API URL for link injection | `""` |
| `database.existingSecret` | DB credentials secret name | `default-pguser-eoapi` |
| `storage.outputPath` | Output path (s3://, abfs:// or local) | `s3://some-bucket/geoparquet` |
| `storage.existingSecret` | S3 credentials secret name | `""` |
| `storage.endpoint` | Custom S3 endpoint URL | `""` |
| `storage.region` | AWS region | `""` |
| `serviceAccountName` | ServiceAccount for export pods | `""` |
| `podLabels` | Extra labels on export pods | `{}` |
| `jobs.complete.schedule` | Complete export cron schedule | `"0 2 1 * *"` |
| `jobs.incremental.schedule` | Incremental export cron schedule | `"0 3 * * *"` |
| `exportConfig.exportAll` | Export all collections from database | `false` |
| `exportConfig.collections[].partition_frequency` | Set to export one file per pgSTAC partition | unset |
| `exportConfig.settings.chunk_size` | Max items per chunk | `8192` |
| `exportConfig.settings.max_chunk_mb` | Max approximate JSON size per chunk (MB) | `64` |
| `exportConfig.settings.statement_timeout` | Postgres statement timeout (ms) | `3600000` |
| `exportConfig.settings.rewrite` | Complete mode replaces existing files | `true` |
| `exportConfig.overrides.<collection>` | Per-collection settings | `{}` |

### Azure Blob Storage (workload identity)

`abfs://` output authenticates with `DefaultAzureCredential`. With AKS workload identity, use a
ServiceAccount federated with a managed identity that can write to the container:

```yaml
storage:
  outputPath: abfs://exports@myaccount.dfs.core.windows.net/geoparquet
serviceAccountName: geoparquet-exporter
podLabels:
  azure.workload.identity/use: "true"
```

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

### Export All Collections

Set `exportConfig.exportAll: true` to automatically export all collections from the pgSTAC database instead of manually specifying them. Useful when collections are added dynamically. `settings` apply to every collection and `overrides` to the named ones (precedence: `settings` < `overrides` < `collections[]` entry).

With `rewrite: false`, complete mode keeps a readable existing file. Empty or unreadable files (e.g. from a failed run) are always rewritten.

## Advanced Options

Additional configuration available in `values.yaml`:
- `exportConfig.settings`, `exportConfig.overrides` - chunk_size, max_chunk_mb, statement_timeout, rewrite
- `jobs` - successfulJobsHistoryLimit, failedJobsHistoryLimit, concurrencyPolicy
- `extraEnv`, `extraVolumes`, `extraVolumeMounts` - Additional resources
- `nodeSelector`, `tolerations`, `affinity` - Pod scheduling
- `podSecurityContext`, `securityContext` - Security settings
