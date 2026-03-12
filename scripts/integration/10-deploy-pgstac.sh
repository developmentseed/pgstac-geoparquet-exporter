#!/usr/bin/env bash
# Deploy and configure pgSTAC

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

log_section "Deploying pgSTAC"

# Deploy PostgresCluster
log_info "Creating PostgresCluster resource"
kubectl apply -f "$FIXTURES_DIR/pgstac-cluster.yaml" -n "$NAMESPACE"

# Wait for pgSTAC instance pods (not backup/repo-host)
log_info "Waiting for pgSTAC instance pods to be ready"
wait_for_pods "postgres-operator.crunchydata.com/cluster=pgstac,postgres-operator.crunchydata.com/instance" "$NAMESPACE" 420

PGSTAC_POD=$(get_pod_name "postgres-operator.crunchydata.com/cluster=pgstac,postgres-operator.crunchydata.com/instance" "$NAMESPACE")
log_success "pgSTAC pod ready: $PGSTAC_POD"

# Create pgstac database
log_info "Creating pgstac database"
kubectl exec -n "$NAMESPACE" "$PGSTAC_POD" -c database -- \
    psql -U postgres -d postgres -c "CREATE DATABASE pgstac;" 2>/dev/null || \
    log_warning "Database may already exist"

# Install prerequisite extensions
log_info "Installing prerequisite extensions"
kubectl exec -n "$NAMESPACE" "$PGSTAC_POD" -c database -- \
    psql -U postgres -d pgstac -c \
    "CREATE EXTENSION IF NOT EXISTS postgis;
     CREATE EXTENSION IF NOT EXISTS btree_gist;
     CREATE EXTENSION IF NOT EXISTS unaccent;"

log_success "Extensions installed"

# Run pgSTAC migration
log_info "Running pgSTAC migration"
kubectl delete job pgstac-migrate -n "$NAMESPACE" --ignore-not-found=true

sed "s/NAMESPACE_PLACEHOLDER/$NAMESPACE/g" "$FIXTURES_DIR/pgstac-migrate-job.yaml" | \
    kubectl apply -n "$NAMESPACE" -f -

log_info "Waiting for migration job to complete (may take several minutes - pypgstac image is ~700MB)"
kubectl wait --for=condition=complete job/pgstac-migrate -n "$NAMESPACE" --timeout=10m || {
    log_error "pgSTAC migration failed"
    kubectl logs -n "$NAMESPACE" job/pgstac-migrate --tail=50 || true
    exit 1
}

log_success "pgSTAC migration complete"

# Verify installation
log_info "Verifying pgSTAC installation"
PGSTAC_VERSION=$(kubectl exec -n "$NAMESPACE" "$PGSTAC_POD" -c database -- \
    psql -U postgres -d pgstac -t -c \
    "SELECT version FROM pgstac.migrations ORDER BY version DESC LIMIT 1;" 2>/dev/null | tr -d '[:space:]')

if [ -z "$PGSTAC_VERSION" ]; then
    log_error "pgSTAC schema not found after migration"
    kubectl exec -n "$NAMESPACE" "$PGSTAC_POD" -c database -- psql -U postgres -d pgstac -c "\dn" || true
    exit 1
fi

log_success "pgSTAC schema installed (version: $PGSTAC_VERSION)"

# Cleanup migration job
kubectl delete job pgstac-migrate -n "$NAMESPACE" --ignore-not-found=true

log_success "pgSTAC deployment complete"
