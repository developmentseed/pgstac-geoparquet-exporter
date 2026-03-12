# Scripts

## integration-test.sh

End-to-end integration test validating the complete export pipeline from pgSTAC to GeoParquet.

### Quick Start

```bash
./scripts/integration-test.sh

# Optional: customize cluster/namespace
CLUSTER_NAME=my-cluster NAMESPACE=my-ns ./scripts/integration-test.sh
```

**Requirements**: kubectl, helm, python3 (pip). k3d auto-installed if no cluster exists.

### What It Tests

1. **Infrastructure**: k3d cluster, PostgreSQL Operator, pgSTAC, MinIO, Helm chart
2. **Data Loading**: Creates test collection + 3 STAC items via pypgstac
3. **Export Jobs**: Triggers complete export (and incremental - expected to fail)
4. **Validation**: Reads GeoParquet files with PyArrow, validates schema and row count

### Expected Output

```
✓ ALL TESTS PASSED SUCCESSFULLY
════════════════════════════════════════════════════════════
  ✓ CronJobs created: 2
  ✓ STAC items loaded: 3
  ✓ Complete export: SUCCESS
  ✓ Geoparquet validation: PASSED
════════════════════════════════════════════════════════════
```

### Test Data

- **Collection**: `test-collection` (STAC 1.0.0, global extent, 2020-2023)
- **Items**: 3 items with polygon geometries, datetime properties, and assets

### Cleanup

```bash
k3d cluster delete pgstac-test
```

### Troubleshooting

**Data loading fails**
```bash
kubectl get pods -n test -l postgres-operator.crunchydata.com/cluster=pgstac
```

**Export job fails**
```bash
kubectl logs -n test job/test-complete-export
```

**Validation fails**
```bash
# Check MinIO
kubectl get pods -n test -l app=minio

# Access MinIO UI
kubectl port-forward -n test svc/minio 9000:9000
# Open http://localhost:9000 (minioadmin/minioadmin)
```

**Python deps fail**: Test skips validation and reports partial success

### Manual Inspection

Keep cluster running and inspect:

```bash
# View resources
kubectl get all -n test

# Trigger manual export
kubectl create job manual-$(date +%s) \
  --from=cronjob/exporter-pgstac-geoparquet-exporter-complete -n test

# View logs
kubectl logs -n test -l app=geoparquet-export --tail=100
```

### CI/CD

```yaml
- name: Integration Tests
  run: ./scripts/integration-test.sh
  timeout-minutes: 15
```

**Runtime**: ~5-8 minutes
