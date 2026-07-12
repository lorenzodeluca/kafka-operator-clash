#!/usr/bin/env bash
set -euo pipefail

CLAIM_FILE="${1:-datastream-claim.yaml}"
NS="${2:-default}"
OUT_DIR="${3:-results/crossplane}"
mkdir -p "$OUT_DIR"

CLAIM_NAME="$(grep '^  name:' "$CLAIM_FILE" | head -n1 | awk '{print $2}')"
TOPIC_NAME="$(grep 'topicName:' "$CLAIM_FILE" | head -n1 | awk '{print $2}')"

echo "[INFO] claim=$CLAIM_NAME topic=$TOPIC_NAME ns=$NS"

# Clean previous run
kubectl delete datastream "$CLAIM_NAME" -n "$NS" --ignore-not-found=true
sleep 2

START_TS="$(date +%s)"
kubectl apply -f "$CLAIM_FILE"

# Wait for claim readiness (timeout kept moderate)
if kubectl wait --for=condition=Ready "datastream/${CLAIM_NAME}" -n "$NS" --timeout=300s; then
  CLAIM_READY="true"
else
  CLAIM_READY="false"
fi

END_TS="$(date +%s)"
TOTAL_SEC="$((END_TS-START_TS))"

# Gather snapshot
kubectl get datastream "$CLAIM_NAME" -n "$NS" -o yaml > "${OUT_DIR}/datastream.yaml" || true
kubectl get xdatastream -o yaml > "${OUT_DIR}/xdatastreams.yaml" || true
kubectl get topics.topic.kafka.crossplane.io -o yaml > "${OUT_DIR}/topics.yaml" || true
kubectl get object.kubernetes.crossplane.io -o yaml > "${OUT_DIR}/objects.yaml" || true
kubectl get configmap,deploy,pods -n "$NS" -o yaml > "${OUT_DIR}/workloads.yaml" || true

# Extract deployment replicas if present
DEPLOY_NAME="${TOPIC_NAME}-consumer"
REPLICAS="$(kubectl get deploy "$DEPLOY_NAME" -n "$NS" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "NA")"

cat > "${OUT_DIR}/summary.txt" <<EOF
framework=crossplane
claim_name=${CLAIM_NAME}
topic_name=${TOPIC_NAME}
namespace=${NS}
claim_ready=${CLAIM_READY}
total_seconds=${TOTAL_SEC}
deployment=${DEPLOY_NAME}
deployment_replicas=${REPLICAS}
timestamp=$(date -Iseconds)
EOF

echo "[OK] Crossplane data collected in ${OUT_DIR}"
cat "${OUT_DIR}/summary.txt"
