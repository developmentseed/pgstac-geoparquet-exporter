#!/usr/bin/env bash
# Deploy MinIO object storage

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

log_section "Deploying MinIO"

# Create MinIO secret
log_info "Creating MinIO credentials secret"
kubectl create secret generic minio-secret -n "$NAMESPACE" \
    --from-literal=accesskey=minioadmin \
    --from-literal=secretkey=minioadmin \
    --dry-run=client -o yaml | kubectl apply -f -

log_success "MinIO secret created"

# Deploy MinIO
log_info "Creating MinIO deployment and service"
kubectl apply -f "$FIXTURES_DIR/minio.yaml" -n "$NAMESPACE"

# Wait for MinIO to be ready
log_info "Waiting for MinIO to be available"
kubectl wait --for=condition=available deployment/minio -n "$NAMESPACE" --timeout=2m

log_success "MinIO deployment complete"

# Create bucket for tests
log_info "Creating test bucket"
kubectl exec -n "$NAMESPACE" deployment/minio -- mc mb myminio/test --ignore-existing

log_success "Test bucket created"
