#!/usr/bin/env bash
# Load test STAC data into pgSTAC

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

log_section "Loading test STAC data"

# Create ConfigMap with test data
log_info "Creating ConfigMap with test STAC data"
kubectl create configmap stac-test-data -n "$NAMESPACE" \
    --from-file=collection.json="$FIXTURES_DIR/test-collection.json" \
    --from-file=items.ndjson="$FIXTURES_DIR/test-items.ndjson" \
    --dry-run=client -o yaml | kubectl apply -f -

log_success "ConfigMap created"

# Create data loader job
log_info "Creating data loader job"
kubectl delete job stac-data-loader -n "$NAMESPACE" --ignore-not-found=true

kubectl apply -f "$FIXTURES_DIR/stac-data-loader-job.yaml" -n "$NAMESPACE"

# Wait for loader job
log_info "Waiting for data loader job to complete"
kubectl wait --for=condition=complete job/stac-data-loader -n "$NAMESPACE" --timeout=3m || {
    log_error "Data loading failed"
    kubectl logs -n "$NAMESPACE" job/stac-data-loader --tail=50
    exit 1
}

log_success "Data loaded successfully"

# Cleanup loader job
kubectl delete job stac-data-loader -n "$NAMESPACE" --ignore-not-found=true

# Verify data
log_info "Verifying data was loaded"
PGSTAC_POD=$(get_pod_name "postgres-operator.crunchydata.com/cluster=pgstac,postgres-operator.crunchydata.com/instance" "$NAMESPACE")

if [ -z "$PGSTAC_POD" ]; then
    log_error "Could not find pgSTAC pod"
    exit 1
fi

log_info "Querying database on pod: $PGSTAC_POD"

# Query and capture both stdout and stderr
ITEM_COUNT=$(kubectl exec -n "$NAMESPACE" "$PGSTAC_POD" -c database -- \
    psql -U postgres -d pgstac -t -c \
    "SELECT count(*) FROM pgstac.items WHERE collection='test-collection';" 2>&1 | tr -d '[:space:]')

# Check for errors in query output
if echo "$ITEM_COUNT" | grep -qi "error\|fatal\|could not"; then
    log_error "Database query failed: $ITEM_COUNT"
    kubectl exec -n "$NAMESPACE" "$PGSTAC_POD" -c database -- \
        psql -U postgres -d pgstac -c "\dt pgstac.*" || true
    exit 1
fi

if [ -z "$ITEM_COUNT" ] || [ "$ITEM_COUNT" != "3" ]; then
    log_error "Expected 3 items in database, found: ${ITEM_COUNT:-none}"
    log_info "Debugging: Raw output was: '$ITEM_COUNT'"
    kubectl exec -n "$NAMESPACE" "$PGSTAC_POD" -c database -- \
        psql -U postgres -d pgstac -c "\dt pgstac.*" || true
    kubectl exec -n "$NAMESPACE" "$PGSTAC_POD" -c database -- \
        psql -U postgres -d pgstac -c "SELECT collection, count(*) FROM pgstac.items GROUP BY collection;" || true
    exit 1
fi

log_success "Items in database: $ITEM_COUNT"
log_success "Test data loading complete"
