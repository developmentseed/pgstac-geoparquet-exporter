#!/usr/bin/env bash
# Common configuration and utilities for integration tests

set -euo pipefail

# Configuration
export CLUSTER_NAME="${CLUSTER_NAME:-pgstac-test}"
export NAMESPACE="${NAMESPACE:-test}"
export SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export FIXTURES_DIR="$SCRIPT_DIR/fixtures"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $*"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $*"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*" >&2
}

log_section() {
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo -e "${BLUE}$*${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

# Wait for pods with retry logic
wait_for_pods() {
    local label=$1
    local namespace=${2:-$NAMESPACE}
    local timeout=${3:-180}
    
    log_info "Waiting for pods with label: $label"
    log_info "This may take several minutes on first run (downloading container images)"
    
    for i in $(seq 1 60); do
        if kubectl get pod -l "$label" -n "$namespace" 2>/dev/null | grep -q .; then
            break
        fi
        sleep 2
    done
    
    log_info "Waiting for pod readiness (timeout: ${timeout}s)..."
    kubectl wait --for=condition=Ready pod -l "$label" -n "$namespace" --timeout="${timeout}s"
}

# Get pod name by label
get_pod_name() {
    local label=$1
    local namespace=${2:-$NAMESPACE}
    
    kubectl get pod -l "$label" -n "$namespace" -o jsonpath='{.items[0].metadata.name}'
}
