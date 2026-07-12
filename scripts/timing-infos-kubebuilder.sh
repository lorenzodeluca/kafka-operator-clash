#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   bash scripts/timing-infos-kubebuilder.sh [CR_NAME] [NAMESPACE]
CR_NAME="${1:-datastream-sample}"
NS="${2:-default}"
KB_RES="datastreams.messaging.kb.lorenzodeluca.it"
DEPLOY_NAME="${CR_NAME}-consumer"

echo "=== Kubebuilder Timing Info ==="
echo "resource=${KB_RES}"
echo "cr_name=${CR_NAME}"
echo "namespace=${NS}"
echo

# Check CR exists
if ! kubectl get "${KB_RES}" "${CR_NAME}" -n "${NS}" >/dev/null 2>&1; then
  echo "[ERROR] ${KB_RES}/${CR_NAME} not found in namespace ${NS}"
  echo "[HINT] Run collect-kubebuilder-data.sh first, or pass the correct CR name."
  exit 1
fi

echo "--- DataStream condition timestamps ---"
kubectl get "${KB_RES}" "${CR_NAME}" -n "${NS}" -o json \
| jq -r '.status.conditions[]? | [.type,.status,.reason,.lastTransitionTime,.message] | @tsv'
echo

echo "--- DataStream creation timestamp ---"
kubectl get "${KB_RES}" "${CR_NAME}" -n "${NS}" -o jsonpath='{.metadata.creationTimestamp}'
echo
echo

if kubectl get deployment "${DEPLOY_NAME}" -n "${NS}" >/dev/null 2>&1; then
  echo "--- Deployment timestamps ---"
  echo -n "deployment.creationTimestamp: "
  kubectl get deployment "${DEPLOY_NAME}" -n "${NS}" -o jsonpath='{.metadata.creationTimestamp}'
  echo

  echo "--- Deployment conditions ---"
  kubectl get deployment "${DEPLOY_NAME}" -n "${NS}" -o json \
  | jq -r '.status.conditions[]? | [.type,.status,.reason,.lastUpdateTime,.lastTransitionTime,.message] | @tsv'
  echo
else
  echo "[WARN] Deployment ${DEPLOY_NAME} not found"
  echo
fi

echo "--- Recent namespace events (last 30) ---"
kubectl get events -n "${NS}" --sort-by=.lastTimestamp | tail -n 30
