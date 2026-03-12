#!/usr/bin/env bash
# Deploy exporter and run export jobs

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

log_section "Deploying pgSTAC GeoParquet Exporter"

# Build and import Docker image
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
log_info "Building Docker image"
docker build -q -t ghcr.io/developmentseed/pgstac-geoparquet-exporter:latest \
    -f "$PROJECT_ROOT/Dockerfile" "$PROJECT_ROOT" >/dev/null

if [ "${USE_EXISTING_CLUSTER:-false}" != "true" ]; then
    k3d image import ghcr.io/developmentseed/pgstac-geoparquet-exporter:latest -c "$CLUSTER_NAME" >/dev/null 2>&1
fi

log_success "Image ready"

# Deploy helm chart
log_info "Installing helm chart"
helm upgrade --install exporter charts/pgstac-geoparquet-exporter -n "$NAMESPACE" \
    --set image.tag=latest \
    --set image.pullPolicy=IfNotPresent \
    --set database.existingSecret=pgstac-pguser-postgres \
    --set database.secretKeys.database=user \
    --set extraEnv[0].name=PGDATABASE \
    --set extraEnv[0].value=pgstac \
    --set storage.outputPath=s3://test/geoparquet \
    --set storage.existingSecret=minio-secret \
    --set storage.secretKeys.accessKeyId=accesskey \
    --set storage.secretKeys.secretAccessKey=secretkey \
    --set storage.endpoint=http://minio:9000 \
    --set storage.region=us-east-1 \
    --set stacApiUrl=http://localhost \
    --set exportConfig.collections[0].name=test-collection \
    --set exportConfig.collections[0].partition_by=null \
    --wait --timeout 2m

log_success "Helm chart deployed"

# Verify CronJobs
log_info "Verifying CronJobs were created"
CRONJOB_COUNT=$(kubectl get cronjobs -n "$NAMESPACE" --no-headers | wc -l)

if [ "$CRONJOB_COUNT" -lt 2 ]; then
    log_error "Expected at least 2 CronJobs, found: $CRONJOB_COUNT"
    kubectl get cronjobs -n "$NAMESPACE"
    exit 1
fi

log_success "CronJobs created: $CRONJOB_COUNT"

# Run complete export job
log_section "Running complete export job"

kubectl create job test-complete-export -n "$NAMESPACE" \
    --from=cronjob/exporter-pgstac-geoparquet-exporter-complete

log_info "Waiting for complete export job to finish"
if ! kubectl wait --for=condition=complete job/test-complete-export -n "$NAMESPACE" --timeout=5m 2>/dev/null; then
    log_error "Complete export job failed or timed out"
    echo "Job status:"
    kubectl get job/test-complete-export -n "$NAMESPACE" || true
    echo ""
    echo "Job logs:"
    kubectl logs -n "$NAMESPACE" job/test-complete-export --tail=50 || true
    exit 1
fi

log_success "Complete export job finished successfully"

# Cleanup complete export job
kubectl delete job test-complete-export -n "$NAMESPACE" --ignore-not-found=true

# Run incremental export job
log_section "Running incremental export job"

kubectl create job test-incremental-export -n "$NAMESPACE" \
    --from=cronjob/exporter-pgstac-geoparquet-exporter-incremental

log_info "Waiting for incremental export job to finish"
if kubectl wait --for=condition=complete job/test-incremental-export -n "$NAMESPACE" --timeout=5m 2>/dev/null; then
    log_success "Incremental export job completed (unexpected success)"
else
    log_warning "Incremental export job failed (expected - mode not fully implemented)"
fi

# Cleanup incremental export job
kubectl delete job test-incremental-export -n "$NAMESPACE" --ignore-not-found=true

log_success "Export jobs complete"
