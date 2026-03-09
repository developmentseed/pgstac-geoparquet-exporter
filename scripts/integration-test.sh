#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-pgstac-test}"
NAMESPACE="${NAMESPACE:-test}"

# Cluster
if ! kubectl cluster-info &>/dev/null; then
    if command -v k3d &>/dev/null; then
        echo "Creating k3d cluster..."
        k3d cluster create "$CLUSTER_NAME" --agents 1 --wait
    else
        echo "Error: No Kubernetes cluster found and k3d is not installed" >&2
        exit 1
    fi
else
    echo "Using existing cluster..."
fi

# PGO
echo "Installing PostgreSQL Operator..."
helm upgrade --install pgo oci://registry.developers.crunchydata.com/crunchydata/pgo \
    --version 5.8.0 --wait --timeout 3m

# Namespace
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# pgSTAC
echo "Deploying pgSTAC..."
cat <<EOF | kubectl apply -f -
apiVersion: postgres-operator.crunchydata.com/v1beta1
kind: PostgresCluster
metadata:
  name: pgstac
  namespace: $NAMESPACE
spec:
  image: registry.developers.crunchydata.com/crunchydata/crunchy-postgres:ubi8-15.10-0
  postgresVersion: 15
  instances:
    - name: instance1
      replicas: 1
      dataVolumeClaimSpec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 1Gi
  backups:
    pgbackrest:
      repos:
      - name: repo1
        volume:
          volumeClaimSpec:
            accessModes: ["ReadWriteOnce"]
            resources:
              requests:
                storage: 1Gi
  users:
    - name: eoapi
      databases: ["pgstac"]
EOF

echo "Waiting for pgSTAC pods..."
for i in {1..60}; do
  if kubectl get pod -l postgres-operator.crunchydata.com/cluster=pgstac -n "$NAMESPACE" 2>/dev/null | grep -q pgstac; then
    break
  fi
  sleep 2
done
kubectl wait --for=condition=Ready pod -l postgres-operator.crunchydata.com/cluster=pgstac -n "$NAMESPACE" --timeout=3m

# pgSTAC extension
PGSTAC_POD=$(kubectl get pod -l postgres-operator.crunchydata.com/cluster=pgstac,postgres-operator.crunchydata.com/instance -n "$NAMESPACE" -o jsonpath='{.items[0].metadata.name}')
echo "Installing extensions..."
for i in {1..30}; do
  if kubectl exec -n "$NAMESPACE" "$PGSTAC_POD" -- psql -U eoapi -d pgstac -c \
    "CREATE EXTENSION IF NOT EXISTS postgis; CREATE EXTENSION IF NOT EXISTS pgstac;" 2>/dev/null; then
    break
  fi
  sleep 2
done

# MinIO
echo "Deploying MinIO..."
kubectl create secret generic minio-secret -n "$NAMESPACE" \
    --from-literal=accesskey=minioadmin \
    --from-literal=secretkey=minioadmin \
    --dry-run=client -o yaml | kubectl apply -f -

cat <<EOF | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
  namespace: $NAMESPACE
spec:
  replicas: 1
  selector:
    matchLabels:
      app: minio
  template:
    metadata:
      labels:
        app: minio
    spec:
      containers:
      - name: minio
        image: minio/minio:latest
        args: ["server", "/data"]
        env:
        - name: MINIO_ROOT_USER
          value: minioadmin
        - name: MINIO_ROOT_PASSWORD
          value: minioadmin
        ports:
        - containerPort: 9000
---
apiVersion: v1
kind: Service
metadata:
  name: minio
  namespace: $NAMESPACE
spec:
  ports:
  - port: 9000
  selector:
    app: minio
EOF

kubectl wait --for=condition=available deployment/minio -n "$NAMESPACE" --timeout=2m

# Deploy chart
echo "Deploying helm chart..."
helm upgrade --install exporter charts/pgstac-geoparquet-exporter -n "$NAMESPACE" \
    --set database.existingSecret=pgstac-pguser-eoapi \
    --set storage.outputPath=s3://test/geoparquet \
    --set storage.existingSecret=minio-secret \
    --set storage.secretKeys.accessKeyId=accesskey \
    --set storage.secretKeys.secretAccessKey=secretkey \
    --set storage.endpoint=http://minio:9000 \
    --set exportConfig.collections[0].name=test-collection \
    --wait --timeout 2m

# Verify
echo "Verifying CronJobs..."
kubectl get cronjobs -n "$NAMESPACE"
[ "$(kubectl get cronjobs -n "$NAMESPACE" --no-headers | wc -l)" -ge 2 ] || exit 1

echo "✓ Tests passed"
echo "Cleanup: k3d cluster delete $CLUSTER_NAME"
