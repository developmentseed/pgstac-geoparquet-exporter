#!/usr/bin/env bash
# Setup Kubernetes cluster and operators

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

log_section "Setting up Kubernetes cluster"

# Check if we should use existing cluster (opt-in only)
if [ "${USE_EXISTING_CLUSTER:-false}" = "true" ]; then
    log_info "Using existing Kubernetes cluster (USE_EXISTING_CLUSTER=true)"
    if ! kubectl cluster-info &>/dev/null; then
        log_error "USE_EXISTING_CLUSTER=true but no cluster is available"
        exit 1
    fi
else
    # Always create a fresh k3d cluster for isolation
    if ! command -v k3d &>/dev/null; then
        log_error "k3d is not installed. Install it or set USE_EXISTING_CLUSTER=true"
        exit 1
    fi
    
    # Delete existing cluster if it exists
    if k3d cluster list | grep -q "^$CLUSTER_NAME "; then
        log_info "Deleting existing k3d cluster: $CLUSTER_NAME"
        k3d cluster delete "$CLUSTER_NAME"
    fi
    
    log_info "Creating fresh k3d cluster: $CLUSTER_NAME"
    k3d cluster create "$CLUSTER_NAME" --agents 1 --wait
    log_success "k3d cluster created"
fi

# Install PostgreSQL Operator
log_section "Installing PostgreSQL Operator"

# Check if already installed and working
if kubectl get deployment pgo -n default &>/dev/null; then
    if kubectl get deployment pgo -n default -o jsonpath='{.status.conditions[?(@.type=="Available")].status}' | grep -q "True"; then
        log_info "PostgreSQL Operator already installed and running"
    else
        log_info "PostgreSQL Operator exists but not ready, waiting..."
        kubectl wait --for=condition=available deployment/pgo -n default --timeout=10m
    fi
else
    log_info "Installing PostgreSQL Operator"
    # Install without --wait to avoid timeout issues, then wait manually
    helm upgrade --install pgo \
        oci://registry.developers.crunchydata.com/crunchydata/pgo \
        --version 6.0.0 \
        --timeout 10m || true
    
    # Wait for deployment to be available (allow time for large image pull ~150MB)
    log_info "Waiting for PostgreSQL Operator to be ready (this may take several minutes for image download)..."
    kubectl wait --for=condition=available deployment/pgo -n default --timeout=10m
fi

log_success "PostgreSQL Operator installed"

# Create namespace
log_info "Creating namespace: $NAMESPACE"

# Wait if namespace is terminating
if kubectl get namespace "$NAMESPACE" &>/dev/null; then
    STATUS=$(kubectl get namespace "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
    if [ "$STATUS" = "Terminating" ]; then
        log_info "Waiting for namespace to finish terminating..."
        kubectl wait --for=delete namespace/"$NAMESPACE" --timeout=2m 2>/dev/null || true
        sleep 2
    fi
fi

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
log_success "Namespace ready"

log_success "Cluster setup complete"
