#!/usr/bin/env bash
# Deploy S3-compatible object storage

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

log_section "Deploying S3 storage"

# Create S3 secret
log_info "Creating S3 credentials secret"
kubectl create secret generic s3-secret -n "$NAMESPACE" \
    --from-literal=accesskey="$S3_ACCESS_KEY" \
    --from-literal=secretkey="$S3_SECRET_KEY" \
    --dry-run=client -o yaml | kubectl apply -f -

log_success "S3 secret created"

# Deploy S3 server
log_info "Creating S3 deployment and service"
kubectl apply -f "$FIXTURES_DIR/s3.yaml" -n "$NAMESPACE"

# Wait for S3 server to be ready
log_info "Waiting for S3 server to be available"
kubectl wait --for=condition=available deployment/s3 -n "$NAMESPACE" --timeout=3m

log_success "S3 deployment complete"

# Create bucket for tests
log_info "Creating test bucket"
kubectl run s3-create-bucket -n "$NAMESPACE" --rm -i --restart=Never \
    --pod-running-timeout=3m \
    --image=amazon/aws-cli:2.37.13 \
    --env=AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY" \
    --env=AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY" \
    --env=AWS_DEFAULT_REGION=us-east-1 \
    --env=AWS_ENDPOINT_URL=http://s3:9000 \
    --command -- sh -c 'aws s3api head-bucket --bucket test 2>/dev/null || aws s3 mb s3://test'

log_success "Test bucket created"
