#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Stage-1 Flink baseline runner.  Implements the environment discipline of
# flink-native/docs/PERF_EXPERIMENT_PROPOSAL.md section 1 on the measurement
# box: official flink-benchmarks JMH classes only (no self-made Flink probes),
# one pinned physical core, SMT sibling offline, env snapshot beside results,
# >=7 measurement iterations.  Each group lands as a JMH csv+json pair; ingest
# with
#   python3 benchctl.py ingest-jmh <csv> --host <box> \
#       --source fabperf-<runid>-<group>
set -euo pipefail

REPO_DIR=${REPO_DIR:-$(cd "$(dirname "$0")/.." && pwd)}
CORE=${CORE:-0}
FLINK_VERSION=${FLINK_VERSION:-2.3-SNAPSHOT}
DATA_TMP=${DATA_TMP:-/mnt/datadisk0/fabperf/tmp}
WARMUP=${WARMUP:-2}
ITERATIONS=${ITERATIONS:-7}
ITER_TIME=${ITER_TIME:-20s}
XMX=${XMX:-4g}
GROUPS=${GROUPS:-window state-memory state-rocks}
JAR=${JAR:-}
ENV_ONLY=${ENV_ONLY:-0}
RUN_ID=${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}
OUT_DIR=${OUT_DIR:-$REPO_DIR/fabperf/results/$RUN_ID}

cd "$REPO_DIR"
mkdir -p "$OUT_DIR" "$DATA_TMP"

# --- control variables --------------------------------------------------------
SIBLINGS=$(cat "/sys/devices/system/cpu/cpu${CORE}/topology/thread_siblings_list")
CPUS=()
IFS=',' read -ra PARTS <<< "$SIBLINGS"
for part in "${PARTS[@]}"; do
  if [[ "$part" == *-* ]]; then
    lo=${part%-*}; hi=${part#*-}
    for ((c=lo; c<=hi; c++)); do CPUS+=("$c"); done
  else
    CPUS+=("$part")
  fi
done
SIBLING=""
for c in "${CPUS[@]}"; do [[ "$c" != "$CORE" ]] && SIBLING=$c; done

restore_sibling() {
  if [[ -n "$SIBLING" ]]; then
    echo 1 | sudo tee "/sys/devices/system/cpu/cpu${SIBLING}/online" >/dev/null 2>&1 || true
    echo "smt sibling cpu${SIBLING} back online"
  fi
}
if [[ -n "$SIBLING" ]] && sudo -n true 2>/dev/null; then
  echo 0 | sudo tee "/sys/devices/system/cpu/cpu${SIBLING}/online" >/dev/null
  trap restore_sibling EXIT
  echo "smt sibling cpu${SIBLING} offline for the run"
else
  echo "WARNING: SMT sibling left online (no passwordless sudo, or core has no sibling)"
fi

snapshot_env() {
  python3 fabperf/env_snapshot.py --out "$OUT_DIR/env.json" --core "$CORE" \
    --flink-version "$FLINK_VERSION" --jar "${JAR:-pending}" \
    --warmup "$WARMUP" --iterations "$ITERATIONS" --iter-time "$ITER_TIME" \
    --xmx "$XMX" --run-id "$RUN_ID" \
    --note "Stage-1 single-core throughput baseline; groups: $GROUPS"
}
snapshot_env

if [[ "$ENV_ONLY" == "1" ]]; then
  echo "ENV_ONLY=1 - snapshot written, no benchmark executed"
  exit 0
fi

# --- jar ----------------------------------------------------------------------
if [[ -z "$JAR" ]]; then
  JAR=$(ls target/flink-benchmarks-*.jar 2>/dev/null | grep -v original | head -1 || true)
  if [[ -z "$JAR" ]]; then
    echo "building benchmarks jar against flink $FLINK_VERSION ..."
    ./mvnw -q -DskipTests -Dflink.version="$FLINK_VERSION" package
    JAR=$(ls target/flink-benchmarks-*.jar | grep -v original | head -1)
  fi
fi
[[ -f "$JAR" ]] || { echo "jar not found: $JAR" >&2; exit 1; }
echo "jar: $JAR"
snapshot_env

# --- groups -------------------------------------------------------------------
run_group() {
  local name=$1 pattern=$2
  echo "=== $name ($(date -u +%H:%M:%SZ)) ==="
  taskset -c "$CORE" java -cp "$JAR" org.openjdk.jmh.Main \
    -foe true -f 1 -wi "$WARMUP" -i "$ITERATIONS" -w "$ITER_TIME" -r "$ITER_TIME" \
    -jvmArgs "-Xmx$XMX -XX:ActiveProcessorCount=1 -Djava.io.tmpdir=$DATA_TMP" \
    -rf csv -rff "$OUT_DIR/$name.csv" \
    -rf json -rff "$OUT_DIR/$name.json" \
    "$pattern" > "$OUT_DIR/$name.log" 2>&1
  tail -4 "$OUT_DIR/$name.log"
}

for group in $GROUPS; do
  case "$group" in
    window)       run_group window "org.apache.flink.benchmark.WindowBenchmarks" ;;
    state-memory) run_group state-memory "org.apache.flink.benchmark.MemoryStateBackendBenchmark" ;;
    state-rocks)  run_group state-rocks "org.apache.flink.benchmark.RocksStateBackendBenchmark" ;;
    *) echo "unknown group: $group" >&2; exit 1 ;;
  esac
done

echo "RUN_ID=$RUN_ID results in $OUT_DIR"
