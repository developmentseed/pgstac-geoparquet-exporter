# Integration Tests

Kubernetes integration test using k3d for pgstac-geoparquet-exporter.

## Requirements

- Docker
- [k3d](https://k3d.io)
- kubectl
- helm

## Run

```bash
./scripts/integration-test.sh
```

## What it does

1. Creates k3d cluster
2. Installs PostgreSQL Operator + pgSTAC database
3. Deploys MinIO (S3)
4. Installs helm chart
5. Verifies CronJobs are created

## Cleanup

```bash
k3d cluster delete pgstac-test
```
