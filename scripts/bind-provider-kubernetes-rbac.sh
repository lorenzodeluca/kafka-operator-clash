#!/usr/bin/env bash

# Exit on:
# -e: any command failure
# -u: use of undefined variable
# -o pipefail: failure in any part of a pipeline
set -euo pipefail

# Script arguments (with defaults):
# 1) Namespace where Crossplane providers run (usually upbound-system)
# 2) Target namespace where app resources are managed (ex. default)
PROVIDER_NS="${1:-upbound-system}"
TARGET_NS="${2:-default}"

echo "[INFO] Discovering provider-kubernetes deployment in namespace: ${PROVIDER_NS}"

# Find the first deployment whose name contains "provider-kubernetes".
# Example output: deployment.apps/provider-kubernetes-4c9c004f2f1e
PK8S_DEPLOY="$(kubectl -n "${PROVIDER_NS}" get deploy -o name | grep provider-kubernetes | head -n1 || true)"

# Defensive check: if not found, stop with clear message.
if [[ -z "${PK8S_DEPLOY}" ]]; then
  echo "[ERROR] provider-kubernetes deployment not found in namespace ${PROVIDER_NS}"
  exit 1
fi

# Extract the ServiceAccount used by that deployment's pod template.
PK8S_SA="$(kubectl -n "${PROVIDER_NS}" get "${PK8S_DEPLOY}" -o jsonpath='{.spec.template.spec.serviceAccountName}')"

# Defensive check: if empty, something is wrong in discovery.
if [[ -z "${PK8S_SA}" ]]; then
  echo "[ERROR] Could not detect ServiceAccount from ${PK8S_DEPLOY}"
  exit 1
fi

echo "[INFO] Found deployment: ${PK8S_DEPLOY}"
echo "[INFO] Found ServiceAccount: ${PK8S_SA}"

# Apply Role manifest (permission set) in target namespace.
echo "[INFO] Applying Role in namespace ${TARGET_NS}"
kubectl apply -f crossplane-config/providers/rbac-provider-kubernetes-default.yaml

# Create/apply RoleBinding dynamically with discovered ServiceAccount.
# This avoids hardcoding provider-generated SA names.
echo "[INFO] Applying RoleBinding in namespace ${TARGET_NS}"
cat <<EOF | kubectl apply -f -
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: provider-kubernetes-manager-binding
  namespace: ${TARGET_NS}
subjects:
  - kind: ServiceAccount
    name: ${PK8S_SA}
    namespace: ${PROVIDER_NS}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: provider-kubernetes-manager
EOF

echo "[OK] RBAC configured."
echo "[OK] provider-kubernetes (${PK8S_SA}) can now manage resources in namespace ${TARGET_NS}."
