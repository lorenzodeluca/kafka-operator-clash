#!/usr/bin/env bash
set -euo pipefail

# Resolve repo root from script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

OP_DIR="${1:-${REPO_ROOT}/kubebuilder-operator}"
SAMPLE_FILE="${2:-${REPO_ROOT}/kubebuilder-operator/config/samples/messaging_v1alpha1_datastream.yaml}"
NS="${3:-default}"
OUT_DIR="${4:-${REPO_ROOT}/results/kubebuilder}"

# Fully-qualified resource for Kubebuilder API group
KB_RES="datastreams.messaging.kb.lorenzodeluca.it"

mkdir -p "${OUT_DIR}"

if [[ ! -f "${SAMPLE_FILE}" ]]; then
  echo "[ERROR] sample file not found: ${SAMPLE_FILE}"
  exit 1
fi

CR_NAME="$(grep '^  name:' "${SAMPLE_FILE}" | head -n1 | awk '{print $2}' || true)"
TOPIC_NAME="$(grep 'topicName:' "${SAMPLE_FILE}" | head -n1 | awk '{print $2}' || true)"

if [[ -z "${CR_NAME}" ]]; then
  echo "[ERROR] could not parse CR name from ${SAMPLE_FILE}"
  exit 1
fi

if [[ -z "${TOPIC_NAME}" ]]; then
  echo "[ERROR] could not parse topicName from ${SAMPLE_FILE}"
  exit 1
fi

# Controller naming convention in your reconciler
DEPLOY_NAME="${CR_NAME}-consumer"
CM_NAME="${CR_NAME}-connection"

echo "[INFO] op_dir=${OP_DIR}"
echo "[INFO] sample=${SAMPLE_FILE}"
echo "[INFO] cr=${CR_NAME} topic=${TOPIC_NAME} deploy=${DEPLOY_NAME} ns=${NS}"

# Optional broker override (important when running controller from host)
# Example:
#   export KAFKA_BROKER=localhost:9092
export KAFKA_BROKER="${KAFKA_BROKER:-kafka.kafka.svc.cluster.local:9092}"
echo "[INFO] KAFKA_BROKER=${KAFKA_BROKER}"

# Prevent hidden failure if 8081 is already occupied
if lsof -i :8081 >/dev/null 2>&1; then
  echo "[ERROR] Port 8081 is already in use. Stop the old controller first."
  lsof -i :8081 || true
  exit 1
fi

# Install CRD/RBAC/manifests
( cd "${OP_DIR}" && make install )

# Start controller in background and capture logs
( cd "${OP_DIR}" && make run ) > "${OUT_DIR}/controller.log" 2>&1 &
CTRL_PID=$!

cleanup() {
  kill "${CTRL_PID}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Give time to start, then verify it's alive
sleep 8
if ! kill -0 "${CTRL_PID}" 2>/dev/null; then
  echo "[ERROR] controller failed to start. Last logs:"
  tail -n 100 "${OUT_DIR}/controller.log" || true
  exit 1
fi

# Clean previous run (kubebuilder group only)
kubectl delete "${KB_RES}" "${CR_NAME}" -n "${NS}" --ignore-not-found=true
kubectl delete deployment "${DEPLOY_NAME}" -n "${NS}" --ignore-not-found=true
kubectl delete configmap "${CM_NAME}" -n "${NS}" --ignore-not-found=true
sleep 2

START_TS="$(date +%s)"
kubectl apply -f "${SAMPLE_FILE}"

# Wait for deployment object creation (up to 120s)
DEPLOY_CREATED="false"
for _ in $(seq 1 60); do
  if kubectl get deployment "${DEPLOY_NAME}" -n "${NS}" >/dev/null 2>&1; then
    DEPLOY_CREATED="true"
    break
  fi
  sleep 2
done

# Wait for deployment availability if created
if [[ "${DEPLOY_CREATED}" == "true" ]]; then
  if kubectl wait --for=condition=Available "deployment/${DEPLOY_NAME}" -n "${NS}" --timeout=300s; then
    DEPLOY_READY="true"
  else
    DEPLOY_READY="false"
  fi
else
  DEPLOY_READY="false"
fi

END_TS="$(date +%s)"
TOTAL_SEC="$((END_TS-START_TS))"

# Gather status and snapshots
kubectl get "${KB_RES}" "${CR_NAME}" -n "${NS}" -o yaml > "${OUT_DIR}/datastream.yaml" || true
kubectl get configmap,deploy,pods -n "${NS}" -o yaml > "${OUT_DIR}/workloads.yaml" || true
kubectl get events -n "${NS}" --sort-by=.lastTimestamp > "${OUT_DIR}/events.txt" || true

REPLICAS="$(kubectl get deploy "${DEPLOY_NAME}" -n "${NS}" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "NA")"
READY_REPLICAS="$(kubectl get deploy "${DEPLOY_NAME}" -n "${NS}" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "NA")"
DS_READY_COND="$(kubectl get "${KB_RES}" "${CR_NAME}" -n "${NS}" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "NA")"
DS_READY_MSG="$(kubectl get "${KB_RES}" "${CR_NAME}" -n "${NS}" -o jsonpath='{.status.conditions[?(@.type=="Ready")].message}' 2>/dev/null || echo "NA")"

cat > "${OUT_DIR}/summary.txt" <<EOF
framework=kubebuilder
resource=${KB_RES}
cr_name=${CR_NAME}
topic_name=${TOPIC_NAME}
namespace=${NS}
kafka_broker=${KAFKA_BROKER}
deployment=${DEPLOY_NAME}
deployment_created=${DEPLOY_CREATED}
deployment_ready=${DEPLOY_READY}
deployment_replicas=${REPLICAS}
deployment_ready_replicas=${READY_REPLICAS}
datastream_ready_condition=${DS_READY_COND}
datastream_ready_message=${DS_READY_MSG}
total_seconds=${TOTAL_SEC}
timestamp=$(date -Iseconds)
EOF

echo "[OK] Kubebuilder data collected in ${OUT_DIR}"
cat "${OUT_DIR}/summary.txt"

if [[ "${DEPLOY_READY}" != "true" ]]; then
  echo
  echo "[WARN] Deployment not ready. Useful diagnostics:"
  echo "  - tail -n 100 ${OUT_DIR}/controller.log"
  echo "  - kubectl describe ${KB_RES} ${CR_NAME} -n ${NS}"
  echo "  - kubectl get deploy,pods -n ${NS}"
fi
