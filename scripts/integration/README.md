# Integration Tests

This directory contains modular integration tests for the pgSTAC GeoParquet Exporter.

## Overview

The integration tests verify end-to-end functionality by:

1. Setting up a Kubernetes cluster (k3d)
2. Deploying PostgreSQL with pgSTAC
3. Deploying MinIO for object storage
4. Loading test STAC data
5. Running export jobs
6. Validating the exported GeoParquet files

## Quick Start

Run all tests:

```bash
./scripts/integration-test.sh
```

Run tests with automatic cleanup:

```bash
./scripts/integration-test.sh --cleanup
```

## Structure

```
integration/
├── common.sh                 # Shared configuration and utilities
├── 00-setup-cluster.sh       # Create k3d cluster and install operators
├── 10-deploy-pgstac.sh       # Deploy and configure pgSTAC
├── 20-deploy-minio.sh        # Deploy MinIO object storage
├── 30-load-test-data.sh      # Load test STAC collection and items
├── 40-run-export.sh          # Deploy exporter and run jobs
├── 50-validate.sh            # Validate exported GeoParquet files
├── cleanup.sh                # Cleanup resources
└── fixtures/                 # YAML and JSON test fixtures
    ├── pgstac-cluster.yaml
    ├── minio.yaml
    ├── test-collection.json
    └── test-items.ndjson
```

## Running Individual Phases

You can run test phases independently for debugging:

```bash
# Setup cluster only
./scripts/integration/00-setup-cluster.sh

# Deploy pgSTAC only (requires cluster)
./scripts/integration/10-deploy-pgstac.sh

# Run validation only (requires all previous phases)
./scripts/integration/50-validate.sh
```

## Environment Variables

- `CLUSTER_NAME` - k3d cluster name (default: `pgstac-test`)
- `NAMESPACE` - Kubernetes namespace (default: `test`)

Example:

```bash
