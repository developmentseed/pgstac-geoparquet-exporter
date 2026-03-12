#!/usr/bin/env bash
# Validate exported GeoParquet files

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

log_section "Validating GeoParquet files"

# Port-forward MinIO to access it locally
log_info "Setting up port-forward to MinIO"
kubectl port-forward -n "$NAMESPACE" svc/minio 9000:9000 &
PF_PID=$!

# Cleanup function
cleanup_port_forward() {
    if [ -n "$PF_PID" ]; then
        log_info "Cleaning up port-forward"
        kill $PF_PID 2>/dev/null || true
    fi
}

# Ensure cleanup on exit
trap cleanup_port_forward EXIT

# Wait for port-forward to be ready
sleep 3

# Install validation dependencies
log_info "Installing Python validation dependencies"
if ! python3 -m pip install --quiet pyarrow s3fs 2>/dev/null; then
    log_warning "Could not install validation dependencies, skipping parquet validation"
    cleanup_port_forward
    trap - EXIT
    log_success "Validation skipped (dependencies unavailable)"
    exit 0
fi

# Run validation
log_info "Running GeoParquet validation"
if ! python3 "$SCRIPT_DIR/../validate_parquet.py" \
    test/geoparquet/test-collection \
    --expected-rows 3; then
    cleanup_port_forward
    trap - EXIT
    log_error "GeoParquet validation failed"
    exit 1
fi

# Cleanup
cleanup_port_forward
trap - EXIT

log_success "GeoParquet validation complete"
