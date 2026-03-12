#!/usr/bin/env bash
# Integration test orchestrator for pgSTAC GeoParquet Exporter
#
# This script runs end-to-end integration tests by orchestrating
# individual test phases. Each phase is in a separate script for
# better maintainability and independent testing.
#
# Usage:
#   ./scripts/integration-test.sh [--cleanup] [--skip-validation]
#
# Environment variables:
#   CLUSTER_NAME          - Name of k3d cluster (default: pgstac-test)
#   NAMESPACE             - Kubernetes namespace (default: test)
#   USE_EXISTING_CLUSTER  - Use existing cluster instead of creating k3d (default: false)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INTEGRATION_DIR="$SCRIPT_DIR/integration"

# Parse arguments
CLEANUP=false
SKIP_VALIDATION=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --cleanup)
            CLEANUP=true
            shift
            ;;
        --skip-validation)
            SKIP_VALIDATION=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --cleanup           Clean up all resources after tests"
            echo "  --skip-validation   Skip GeoParquet validation step"
            echo "  -h, --help          Show this help message"
            echo ""
            echo "Environment variables:"
            echo "  CLUSTER_NAME          k3d cluster name (default: pgstac-test)"
            echo "  NAMESPACE             Kubernetes namespace (default: test)"
            echo "  USE_EXISTING_CLUSTER  Use existing cluster instead of k3d (default: false)"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Run with --help for usage information"
            exit 1
            ;;
    esac
done

# Source common utilities
source "$INTEGRATION_DIR/common.sh"

# Cleanup function
cleanup_on_exit() {
    local exit_code=$?

    if [ $exit_code -ne 0 ]; then
        echo ""
        log_error "Integration tests failed (exit code: $exit_code)"
        echo ""
        log_info "To debug, resources are still running. You can:"
        echo "  - Check logs: kubectl logs -n $NAMESPACE <pod-name>"
        echo "  - Inspect resources: kubectl get all -n $NAMESPACE"
        echo "  - Clean up: $INTEGRATION_DIR/cleanup.sh --all"
    fi

    if [ "$CLEANUP" = true ]; then
        echo ""
        log_info "Running cleanup..."
        "$INTEGRATION_DIR/cleanup.sh" --all
    fi
}

trap cleanup_on_exit EXIT

# Print header
echo "════════════════════════════════════════════════════════════"
echo "  pgSTAC GeoParquet Exporter - Integration Tests"
echo "════════════════════════════════════════════════════════════"
echo ""
log_info "Cluster: $CLUSTER_NAME"
log_info "Namespace: $NAMESPACE"
echo ""

# Run test phases
"$INTEGRATION_DIR/00-setup-cluster.sh"
"$INTEGRATION_DIR/10-deploy-pgstac.sh"
"$INTEGRATION_DIR/20-deploy-minio.sh"
"$INTEGRATION_DIR/30-load-test-data.sh"
"$INTEGRATION_DIR/40-run-export.sh"

if [ "$SKIP_VALIDATION" != true ]; then
    "$INTEGRATION_DIR/50-validate.sh"
else
    log_warning "Skipping GeoParquet validation (--skip-validation)"
fi

# Print success summary
echo ""
echo "════════════════════════════════════════════════════════════"
log_success "ALL TESTS PASSED SUCCESSFULLY"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "  ✓ Cluster: $CLUSTER_NAME"
echo "  ✓ Namespace: $NAMESPACE"
echo "  ✓ CronJobs deployed"
echo "  ✓ STAC data loaded (3 items)"
echo "  ✓ Complete export: SUCCESS"
if [ "$SKIP_VALIDATION" != true ]; then
    echo "  ✓ GeoParquet validation: PASSED"
fi
echo ""

if [ "$CLEANUP" != true ]; then
    log_info "Resources are still running. To clean up:"
    echo "  $INTEGRATION_DIR/cleanup.sh --all"
    echo ""
    echo "Or re-run with: $0 --cleanup"
fi
