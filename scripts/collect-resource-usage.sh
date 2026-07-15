#!/usr/bin/env bash
#
# collect-resource-usage.sh
#
# Collects CPU/memory samples for the two "resource consumption" charts:
#   1. at-rest vs under-load bar chart, per component
#   2. scaling line chart (resource usage vs number of DataStream resources)
#
# Components sampled:
#   - crossplane-core                  (pod, namespace: $CP_SYSTEM_NS)
#   - provider-kafka                   (pod, namespace: $CP_SYSTEM_NS)
#   - provider-kubernetes              (pod, namespace: $CP_SYSTEM_NS)
#   - function-patch-and-transform     (pod, namespace: $CP_SYSTEM_NS)
#   - kubebuilder-controller           (local process started via `make run`)
#
# Output: a single CSV, appended across runs, with columns:
#   timestamp,mode,framework,n_resources,component,cpu_millicores,mem_mib
#
# Requirements: kubectl, jq, metrics-server (kubectl top must work), and for
# kubebuilder scaling/at-rest runs: `make` available in KB_OP_DIR.
#
# Usage:
#   ./collect-resource-usage.sh at-rest [DURATION] [INTERVAL]
#   ./collect-resource-usage.sh scale <crossplane|kubebuilder> <n1,n2,n3,...> [DURATION] [INTERVAL]
#
# Examples:
#   ./collect-resource-usage.sh at-rest 60 5
#   ./collect-resource-usage.sh scale crossplane 1,10,50,100 60 5
#   ./collect-resource-usage.sh scale kubebuilder 1,10,50,100 60 5
#
set -euo pipefail

# --- config (override via env) ------------------------------------------
NS="${NS:-default}"                             # namespace where DataStream/XDataStream resources live
CP_SYSTEM_NS="${CP_SYSTEM_NS:-upbound-system}" # namespace where Crossplane/providers/functions run
KB_OP_DIR="${KB_OP_DIR:-./kubebuilder-operator}"
OUT_DIR="${OUT_DIR:-results/resource-usage}"
SAMPLE_CSV="${OUT_DIR}/samples.csv"

CP_CLAIM_RES="datastreams.messaging.lorenzodeluca.it"
KB_RES="datastreams.messaging.kb.lorenzodeluca.it"

KB_PID=""          # PID actually sampled (resolved leaf process of `make run`)
KB_WRAPPER_PID=""  # PID returned by $! for the backgrounded `make run` job
BURST_PID=""       # PID of background burst sampler

mkdir -p "${OUT_DIR}"

# --- setup / cleanup -------------------------------------------------------

check_prereqs() {
  for bin in kubectl jq awk; do
    command -v "${bin}" >/dev/null 2>&1 || { echo "[ERROR] ${bin} not found in PATH"; exit 1; }
  done
  if ! kubectl top nodes >/dev/null 2>&1; then
    echo "[ERROR] 'kubectl top' failed - metrics-server not available/ready"
    exit 1
  fi
}

ensure_csv_header() {
  if [[ ! -f "${SAMPLE_CSV}" ]]; then
    echo "timestamp,mode,framework,n_resources,component,cpu_millicores,mem_mib" > "${SAMPLE_CSV}"
  fi
}

cleanup() {
  stop_burst_sampler "${BURST_PID}"
  stop_kubebuilder_controller
  delete_by_prefix "${CP_CLAIM_RES}" "${NS}" "bench-"
  delete_by_prefix "${KB_RES}" "${NS}" "bench-"
}
trap cleanup EXIT

delete_by_prefix() {
  local resource="$1" ns="$2" prefix="$3"
  kubectl get "${resource}" -n "${ns}" -o name 2>/dev/null \
    | grep "/${prefix}" \
    | xargs -r kubectl delete -n "${ns}" >/dev/null 2>&1 || true
}

delete_nonbench_kb_resources() {
  # Remove pre-existing kubebuilder DataStreams that are NOT part of this benchmark.
  # This avoids queueing/slow reconciles on old resources (e.g. datastream-sample).
  kubectl get "${KB_RES}" -n "${NS}" -o name 2>/dev/null \
    | grep -v '/bench-' \
    | xargs -r kubectl delete -n "${NS}" >/dev/null 2>&1 || true
}

warn_if_preexisting_resources() {
  local cp_count kb_count
  cp_count="$(kubectl get "${CP_CLAIM_RES}" -n "${NS}" -o name 2>/dev/null | grep -vc '/bench-' || true)"
  kb_count="$(kubectl get "${KB_RES}" -n "${NS}" -o name 2>/dev/null | grep -vc '/bench-' || true)"
  if [[ "${cp_count}" -gt 0 || "${kb_count}" -gt 0 ]]; then
    echo "[WARN] found pre-existing DataStream resources outside the 'bench-' prefix"
    echo "       (crossplane: ${cp_count}, kubebuilder: ${kb_count}) - these can skew baselines/scale timings."
    echo "       Consider deleting them manually before running tests."
  fi
}

# --- kubebuilder controller lifecycle ---------------------------------------

find_deepest_pid() {
  local pid="$1"
  local child
  child="$(pgrep -P "${pid}" 2>/dev/null | head -n1 || true)"
  if [[ -z "${child}" ]]; then
    echo "${pid}"
    return
  fi
  find_deepest_pid "${child}"
}

kill_tree() {
  local pid="$1"
  local children
  children="$(pgrep -P "${pid}" 2>/dev/null || true)"
  local child
  for child in ${children}; do
    kill_tree "${child}"
  done
  kill "${pid}" 2>/dev/null || true
}

resolve_kubebuilder_pid() {
  local wrapper_pid="$1"
  local attempt resolved rss_kb
  for attempt in 1 2 3 4 5 6; do
    resolved="$(find_deepest_pid "${wrapper_pid}")"
    if [[ -n "${resolved}" && -d "/proc/${resolved}" ]]; then
      rss_kb="$(awk '/VmRSS/{print $2}' "/proc/${resolved}/status" 2>/dev/null || echo 0)"
      if [[ "${rss_kb}" -gt 5000 ]]; then
        echo "${resolved}"
        return 0
      fi
    fi
    sleep 2
  done
  echo "[WARN] could not confidently resolve controller binary PID - falling back to wrapper PID ${wrapper_pid}" >&2
  echo "${wrapper_pid}"
}

start_kubebuilder_controller() {
  if lsof -i :8081 >/dev/null 2>&1; then
    echo "[ERROR] port 8081 already in use - stop the existing controller first"
    lsof -i :8081 || true
    exit 1
  fi

  local kb_broker="${KAFKA_BROKER:-localhost:9092}"
  echo "[INFO] installing CRD/RBAC and starting kubebuilder controller"
  echo "[INFO] KAFKA_BROKER=${kb_broker}"

  ( cd "${KB_OP_DIR}" && make install ) >/dev/null
  ( cd "${KB_OP_DIR}" && KAFKA_BROKER="${kb_broker}" make run ) > "${OUT_DIR}/controller.log" 2>&1 &
  KB_WRAPPER_PID=$!

  sleep 8
  if ! kill -0 "${KB_WRAPPER_PID}" 2>/dev/null; then
    echo "[ERROR] kubebuilder controller failed to start, see ${OUT_DIR}/controller.log"
    tail -n 80 "${OUT_DIR}/controller.log" || true
    exit 1
  fi

  KB_PID="$(resolve_kubebuilder_pid "${KB_WRAPPER_PID}")"
  local rss_kb
  rss_kb="$(awk '/VmRSS/{print $2}' "/proc/${KB_PID}/status" 2>/dev/null || echo 0)"
  echo "[INFO] kubebuilder controller running, wrapper_pid=${KB_WRAPPER_PID} sampled_pid=${KB_PID} rss=${rss_kb}KB"
}

stop_kubebuilder_controller() {
  if [[ -n "${KB_WRAPPER_PID}" ]] && kill -0 "${KB_WRAPPER_PID}" 2>/dev/null; then
    kill_tree "${KB_WRAPPER_PID}"
    wait "${KB_WRAPPER_PID}" 2>/dev/null || true
    echo "[INFO] kubebuilder controller stopped"
  fi
  KB_PID=""
  KB_WRAPPER_PID=""
}

# --- sampling ----------------------------------------------------------

sample_pods() {
  local component="$1" include_re="$2" exclude_re="${3:-}"
  local mode="$4" framework="$5" n_res="$6"
  local ts names
  ts="$(date -Iseconds)"

  names="$(kubectl get pods -n "${CP_SYSTEM_NS}" --no-headers -o custom-columns=NAME:.metadata.name 2>/dev/null \
    | grep -E "${include_re}" || true)"
  if [[ -n "${exclude_re}" ]]; then
    names="$(echo "${names}" | grep -vE "${exclude_re}" || true)"
  fi
  if [[ -z "${names}" ]]; then
    echo "[WARN] no pods matched component=${component} in ns=${CP_SYSTEM_NS}" >&2
    return 0
  fi

  local total_cpu_m=0 total_mem_mi=0
  while IFS= read -r pod; do
    [[ -z "${pod}" ]] && continue
    local line cpu_raw mem_raw cpu_m mem_mi
    line="$(kubectl top pod "${pod}" -n "${CP_SYSTEM_NS}" --no-headers 2>/dev/null || true)"
    [[ -z "${line}" ]] && continue
    cpu_raw="$(echo "${line}" | awk '{print $2}')"
    mem_raw="$(echo "${line}" | awk '{print $3}')"
    cpu_m="$(echo "${cpu_raw}" | sed 's/m$//')"
    if [[ "${mem_raw}" == *Gi ]]; then
      mem_mi="$(echo "${mem_raw}" | sed 's/Gi$//' | awk '{printf "%d", $1*1024}')"
    else
      mem_mi="$(echo "${mem_raw}" | sed 's/Mi$//')"
    fi
    total_cpu_m=$(( total_cpu_m + cpu_m ))
    total_mem_mi=$(( total_mem_mi + mem_mi ))
  done <<< "${names}"

  echo "${ts},${mode},${framework},${n_res},${component},${total_cpu_m},${total_mem_mi}" >> "${SAMPLE_CSV}"
}

sample_process() {
  local pid="$1" component="$2" mode="$3" framework="$4" n_res="$5"
  local ts
  ts="$(date -Iseconds)"

  if [[ -z "${pid}" ]] || [[ ! -d "/proc/${pid}" ]]; then
    echo "[WARN] pid not found for component=${component}" >&2
    return 0
  fi

  local clk_tck utime1 stime1 t1 utime2 stime2 t2
  clk_tck="$(getconf CLK_TCK)"
  read -r utime1 stime1 <<< "$(awk '{print $14, $15}' "/proc/${pid}/stat")"
  t1="$(date +%s.%N)"
  sleep 1
  read -r utime2 stime2 <<< "$(awk '{print $14, $15}' "/proc/${pid}/stat" 2>/dev/null || echo "0 0")"
  t2="$(date +%s.%N)"

  local d_ticks d_secs cpu_pct cpu_m
  d_ticks=$(( (utime2 - utime1) + (stime2 - stime1) ))
  d_secs="$(awk -v a="${t2}" -v b="${t1}" 'BEGIN{print a-b}')"
  cpu_pct="$(awk -v dt="${d_ticks}" -v ds="${d_secs}" -v tck="${clk_tck}" \
    'BEGIN{ if (ds<=0) print 0; else printf "%.2f", (dt/tck)/ds*100 }')"
  cpu_m="$(awk -v p="${cpu_pct}" 'BEGIN{printf "%.0f", p*10}')"

  local mem_kb mem_mi
  mem_kb="$(awk '/VmRSS/{print $2}' "/proc/${pid}/status" 2>/dev/null || echo 0)"
  mem_mi="$(awk -v k="${mem_kb}" 'BEGIN{printf "%.1f", k/1024}')"

  echo "${ts},${mode},${framework},${n_res},${component},${cpu_m},${mem_mi}" >> "${SAMPLE_CSV}"
}

run_sampling_window() {
  local mode="$1" framework="$2" n_res="$3" duration="$4" interval="$5"
  local elapsed=0
  echo "[INFO] sampling mode=${mode} framework=${framework} n=${n_res} for ${duration}s (every ${interval}s)"
  while (( elapsed < duration )); do
    if [[ "${framework}" == "crossplane" || "${framework}" == "all" ]]; then
      sample_pods "crossplane-core" '^crossplane-' 'rbac-manager|provider-|function-' "${mode}" "${framework}" "${n_res}"
      sample_pods "provider-kafka" 'provider-kafka' '' "${mode}" "${framework}" "${n_res}"
      sample_pods "provider-kubernetes" 'provider-kubernetes' '' "${mode}" "${framework}" "${n_res}"
      sample_pods "function-patch-and-transform" 'function-patch-and-transform' '' "${mode}" "${framework}" "${n_res}"
    fi
    if [[ "${framework}" == "kubebuilder" || "${framework}" == "all" ]]; then
      sample_process "${KB_PID}" "kubebuilder-controller" "${mode}" "${framework}" "${n_res}"
    fi
    sleep "${interval}"
    elapsed=$(( elapsed + interval ))
  done
}

# --- burst sampling ----------------------------------------------------

background_sample_kubebuilder() {
  local n_res="$1"
  while true; do
    sample_process "${KB_PID}" "kubebuilder-controller" "scale-burst" "kubebuilder" "${n_res}"
  done
}

background_sample_crossplane() {
  local n_res="$1" interval="$2"
  while true; do
    sample_pods "crossplane-core" '^crossplane-' 'rbac-manager|provider-|function-' "scale-burst" "crossplane" "${n_res}"
    sample_pods "provider-kafka" 'provider-kafka' '' "scale-burst" "crossplane" "${n_res}"
    sample_pods "provider-kubernetes" 'provider-kubernetes' '' "scale-burst" "crossplane" "${n_res}"
    sample_pods "function-patch-and-transform" 'function-patch-and-transform' '' "scale-burst" "crossplane" "${n_res}"
    sleep "${interval}"
  done
}

start_burst_sampler() {
  local framework="$1" n_res="$2" interval="$3"
  if [[ "${framework}" == "kubebuilder" ]]; then
    background_sample_kubebuilder "${n_res}" &
  else
    background_sample_crossplane "${n_res}" "${interval}" &
  fi
  BURST_PID=$!
}

stop_burst_sampler() {
  local pid="${1:-}"
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
  fi
  BURST_PID=""
}

# --- resource creation for scaling test ---------------------------------

create_crossplane_claims() {
  local n="$1" prefix="$2"
  for ((i=1; i<=n; i++)); do
    cat <<EOF | kubectl apply -f - >/dev/null
apiVersion: messaging.lorenzodeluca.it/v1alpha1
kind: DataStream
metadata:
  name: ${prefix}-${i}
  namespace: ${NS}
spec:
  topicName: "${prefix}-${i}"
  partitions: 1
  replicationFactor: 1
EOF
  done
}

create_kubebuilder_crs() {
  local n="$1" prefix="$2"
  for ((i=1; i<=n; i++)); do
    cat <<EOF | kubectl apply -f - >/dev/null
apiVersion: messaging.kb.lorenzodeluca.it/v1alpha1
kind: DataStream
metadata:
  name: ${prefix}-${i}
  namespace: ${NS}
spec:
  topicName: "${prefix}-${i}"
  partitions: 1
  replicationFactor: 1
EOF
  done
}

wait_for_n_ready() {
  local resource="$1" prefix="$2" n="$3" timeout="${4:-300}"
  local waited=0 ready_count=0 total_count=0

  while (( waited < timeout )); do
    total_count="$(kubectl get "${resource}" -n "${NS}" -o json 2>/dev/null \
      | jq --arg p "${prefix}-" '[.items[] | select(.metadata.name | startswith($p))] | length')"

    ready_count="$(kubectl get "${resource}" -n "${NS}" -o json 2>/dev/null \
      | jq --arg p "${prefix}-" '
        [.items[]
          | select(.metadata.name | startswith($p))
          | ((.status.conditions // []) | map(select(.type=="Ready" and .status=="True")) | length)
        ] | add // 0
      ')"

    if [[ "${ready_count}" -ge "${n}" ]]; then
      echo "[INFO] Ready ${ready_count}/${n} for ${resource} (total created=${total_count})"
      return 0
    fi

    echo "[INFO] waiting Ready ${ready_count}/${n} for ${resource} (created=${total_count}, waited=${waited}s/${timeout}s)"
    sleep 5
    waited=$(( waited + 5 ))
  done

  echo "[WARN] timeout waiting for ${n} ${resource} to be Ready (ready=${ready_count}, created=${total_count})"
  echo "[WARN] Recent matching resources:"
  kubectl get "${resource}" -n "${NS}" -o wide 2>/dev/null | grep "${prefix}-" || true
  return 1
}

# --- subcommands ---------------------------------------------------------

cmd_at_rest() {
  local duration="${1:-60}" interval="${2:-5}"
  check_prereqs
  ensure_csv_header
  warn_if_preexisting_resources

  echo "[INFO] cleaning any leftover bench-* resources before baseline"
  delete_by_prefix "${CP_CLAIM_RES}" "${NS}" "bench-"
  delete_by_prefix "${KB_RES}" "${NS}" "bench-"

  start_kubebuilder_controller
  echo "[INFO] letting controller settle for 10s before sampling"
  sleep 10

  run_sampling_window "at-rest" "all" 0 "${duration}" "${interval}"

  stop_kubebuilder_controller
  echo "[OK] at-rest sampling complete, appended to ${SAMPLE_CSV}"
}

cmd_scale() {
  local framework="$1" counts_csv="$2" duration="${3:-60}" interval="${4:-5}"
  check_prereqs
  ensure_csv_header

  if [[ "${framework}" != "crossplane" && "${framework}" != "kubebuilder" ]]; then
    echo "[ERROR] framework must be 'crossplane' or 'kubebuilder'"
    exit 1
  fi

  local run_id prefix
  run_id="$(date +%s)"
  prefix="bench-${framework}-${run_id}"

  if [[ "${framework}" == "kubebuilder" ]]; then
    echo "[INFO] deleting pre-existing non-bench kubebuilder DataStreams to avoid queue interference"
    delete_nonbench_kb_resources
    start_kubebuilder_controller
    sleep 5
  fi

  IFS=',' read -ra COUNTS <<< "${counts_csv}"
  for n in "${COUNTS[@]}"; do
    echo "[INFO] === scaling test: framework=${framework} n=${n} ==="

    start_burst_sampler "${framework}" "${n}" "${interval}"

    if [[ "${framework}" == "crossplane" ]]; then
      create_crossplane_claims "${n}" "${prefix}"
      echo "[DEBUG] created crossplane resources for n=${n}"
      kubectl get "${CP_CLAIM_RES}" -n "${NS}" 2>/dev/null | grep "${prefix}-" || true
      wait_for_n_ready "${CP_CLAIM_RES}" "${prefix}" "${n}" || true
    else
      create_kubebuilder_crs "${n}" "${prefix}"
      echo "[DEBUG] created kubebuilder resources for n=${n}"
      kubectl get "${KB_RES}" -n "${NS}" 2>/dev/null | grep "${prefix}-" || true
      wait_for_n_ready "${KB_RES}" "${prefix}" "${n}" || true
    fi

    stop_burst_sampler "${BURST_PID}"

    echo "[INFO] letting steady state settle for 10s"
    sleep 10

    run_sampling_window "scale" "${framework}" "${n}" "${duration}" "${interval}"

    echo "[INFO] cleaning up n=${n} resources before next count"
    if [[ "${framework}" == "crossplane" ]]; then
      delete_by_prefix "${CP_CLAIM_RES}" "${NS}" "${prefix}"
    else
      delete_by_prefix "${KB_RES}" "${NS}" "${prefix}"
    fi
    sleep 5
  done

  if [[ "${framework}" == "kubebuilder" ]]; then
    stop_kubebuilder_controller
  fi
  echo "[OK] scaling sampling complete for ${framework}, appended to ${SAMPLE_CSV}"
}

# --- entrypoint -----------------------------------------------------------

usage() {
  cat <<EOF
Usage:
  $0 at-rest [DURATION] [INTERVAL]
  $0 scale <crossplane|kubebuilder> <n1,n2,n3,...> [DURATION] [INTERVAL]

Examples:
  $0 at-rest 60 5
  $0 scale crossplane 1,10,50,100 60 5
  $0 scale kubebuilder 1,10,50,100 60 5
EOF
}

main() {
  local cmd="${1:-}"
  case "${cmd}" in
    at-rest)
      shift
      cmd_at_rest "$@"
      ;;
    scale)
      shift
      if [[ $# -lt 2 ]]; then usage; exit 1; fi
      cmd_scale "$@"
      ;;
    *)
      usage
      exit 1
      ;;
  esac
}

main "$@"
