#!/usr/bin/env bash
set -euo pipefail

CLAIM_FILE="${1:-datastream-claim.yaml}"
NS="${2:-default}"
OUT_DIR="${3:-results/crossplane}"
mkdir -p "${OUT_DIR}"

# Fully-qualified Crossplane resources to avoid CRD name collision with Kubebuilder
XP_CLAIM_RES="datastreams.messaging.lorenzodeluca.it"
XP_XR_RES="xdatastreams.messaging.lorenzodeluca.it"

# Enable/disable unique topic per run:
#   UNIQUE_TOPIC=true  -> topic becomes "<base>-<timestamp>"
#   UNIQUE_TOPIC=false -> keep topic as declared in claim file
UNIQUE_TOPIC="${UNIQUE_TOPIC:-true}"

# Parse claim name and base topic
CLAIM_NAME="$(awk '/^  name:/{print $2; exit}' "${CLAIM_FILE}" | tr -d '"')"
BASE_TOPIC="$(awk '/topicName:/{print $2; exit}' "${CLAIM_FILE}" | tr -d '"')"

if [[ -z "${CLAIM_NAME}" || -z "${BASE_TOPIC}" ]]; then
  echo "[ERROR] Failed to parse claim name or topicName from ${CLAIM_FILE}"
  exit 1
fi

TOPIC_NAME="${BASE_TOPIC}"
TMP_CLAIM="$(mktemp)"
trap 'rm -f "${TMP_CLAIM}"' EXIT

if [[ "${UNIQUE_TOPIC}" == "true" ]]; then
  RUN_ID="$(date +%s)"
  TOPIC_NAME="${BASE_TOPIC}-${RUN_ID}"

  # Rewrite only the first topicName field in a temporary claim file
  awk -v newtopic="${TOPIC_NAME}" '
    BEGIN { replaced=0 }
    {
      if (!replaced && $1 == "topicName:") {
        print "  topicName: \"" newtopic "\""
        replaced=1
        next
      }
      print
    }
  ' "${CLAIM_FILE}" > "${TMP_CLAIM}"

  CLAIM_TO_APPLY="${TMP_CLAIM}"
else
  cp "${CLAIM_FILE}" "${TMP_CLAIM}"
  CLAIM_TO_APPLY="${TMP_CLAIM}"
fi

echo "[INFO] claim=${CLAIM_NAME} base_topic=${BASE_TOPIC} run_topic=${TOPIC_NAME} ns=${NS}"

# Clean previous claim with same name
kubectl delete "${XP_CLAIM_RES}" "${CLAIM_NAME}" -n "${NS}" --ignore-not-found=true
sleep 2

START_TS="$(date +%s)"
kubectl apply -f "${CLAIM_TO_APPLY}"

# Wait for Crossplane claim Ready condition
if kubectl wait \
  --for=jsonpath='{.status.conditions[?(@.type=="Ready")].status}'=True \
  "${XP_CLAIM_RES}/${CLAIM_NAME}" -n "${NS}" --timeout=300s; then
  CLAIM_READY="true"
else
  CLAIM_READY="false"
fi

END_TS="$(date +%s)"
TOTAL_SEC="$((END_TS-START_TS))"

# Collect artifacts
kubectl get "${XP_CLAIM_RES}" "${CLAIM_NAME}" -n "${NS}" -o yaml > "${OUT_DIR}/datastream.yaml" || true
kubectl get "${XP_XR_RES}" -o yaml > "${OUT_DIR}/xdatastreams.yaml" || true
kubectl get topics.topic.kafka.crossplane.io -o yaml > "${OUT_DIR}/topics.yaml" || true
kubectl get objects.kubernetes.crossplane.io -o yaml > "${OUT_DIR}/objects.yaml" || true
kubectl get configmap,deploy,pods -n "${NS}" -o yaml > "${OUT_DIR}/workloads.yaml" || true

DEPLOY_NAME="${TOPIC_NAME}-consumer"
REPLICAS="$(kubectl get deploy "${DEPLOY_NAME}" -n "${NS}" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "NA")"

cat > "${OUT_DIR}/summary.txt" <<EOT
framework=crossplane
claim_name=${CLAIM_NAME}
base_topic_name=${BASE_TOPIC}
topic_name=${TOPIC_NAME}
namespace=${NS}
unique_topic=${UNIQUE_TOPIC}
claim_ready=${CLAIM_READY}
total_seconds=${TOTAL_SEC}
deployment=${DEPLOY_NAME}
deployment_replicas=${REPLICAS}
timestamp=$(date -Iseconds)
EOT

echo "[OK] Crossplane data collected in ${OUT_DIR}"
cat "${OUT_DIR}/summary.txt"
