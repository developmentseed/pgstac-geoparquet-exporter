#!/usr/bin/env bash
# Cleanup integration test resources

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

# Parse arguments
CLEAN_NAMESPACE=${CLEAN_NAMESPACE:-false}
CLEAN_CLUSTER=${CLEAN_CLUSTER:-false}

while [[ $# -gt 0 ]]; do
    case $1 in
        --namespace)
            CLEAN_NAMESPACE=true
            shift
            ;;
        --cluster)
            CLEAN_CLUSTER=true
            shift
            ;;
        --all)
            CLEAN_NAMESPACE=true
            CLEAN_CLUSTER=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--namespace] [--cluster] [--all]"
            exit 1
            ;;
    esac
done

log_section "Cleaning up integration test resources"

# Clean up test resources
log_info "Cleaning up test resources in namespace: $NAMESPACE"

kubectl delete configmap stac-test-data -n "$NAMESPACE" --ignore-not-found=true
kubectl delete job pgstac-migrate -n "$NAMESPACE" --ignore-not-found=true
kubectl delete job stac-data-loader -n "$NAMESPACE" --ignore-not-found=true
kubectl delete job test-complete-export -n "$NAMESPACE" --ignore-not-found=true
kubectl delete job test-incremental-export -n "$NAMESPACE" --ignore-not-found=true

log_success "Test resources cleaned up"

# Clean up namespace if requested
if [ "$CLEAN_NAMESPACE" = true ]; then
    log_info "Deleting namespace: $NAMESPACE"
    kubectl delete namespace "$NAMESPACE" --ignore-not-found=true --timeout=2m
    log_success "Namespace deleted"
fi

# Clean up cluster if requested
if [ "$CLEAN_CLUSTER" = true ]; then
    if command -v k3d &>/dev/null; then
        log_info "Deleting k3d cluster: $CLUSTER_NAME"
        k3d cluster delete "$CLUSTER_NAME" 2>/dev/null || log_warning "Cluster may not exist"
        log_success "k3d cluster deleted"
    else
        log_warning "k3d not available, skipping cluster cleanup"
    fi
fi

log_success "Cleanup complete"

if [ "$CLEAN_NAMESPACE" != true ] && [ "$CLEAN_CLUSTER" != true ]; then
    echo ""
    log_info "To fully clean up, run:"
    echo "  $0 --namespace    # Delete namespace $NAMESPACE"
    echo "  $0 --cluster      # Delete k3d cluster $CLUSTER_NAME"
    echo "  $0 --all          # Delete both"
fi
