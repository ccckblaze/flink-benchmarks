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
#
# Groups: window (WindowBenchmarks), cardinality
# (KeyedStateCardinalityBenchmark, the corrected keyed-state sweep), state-memory and state-rocks
# (the official state group, kept only for continuity: it never changes backend, see README).
set -euo pipefail

REPO_DIR=${REPO_DIR:-$(cd "$(dirname "$0")/.." && pwd)}
CORE=${CORE:-0}
FLINK_VERSION=${FLINK_VERSION:-2.3.0}
DATA_TMP=${DATA_TMP:-/mnt/datadisk0/fabperf/tmp}
WARMUP=${WARMUP:-2}
ITERATIONS=${ITERATIONS:-7}
ITER_TIME=${ITER_TIME:-20s}
XMX=${XMX:-4g}
BENCH_GROUPS=${BENCH_GROUPS:-window cardinality}
CLOCK_CALIB=${CLOCK_CALIB:-1}
JAR=${JAR:-}
ENV_ONLY=${ENV_ONLY:-0}
RUN_ID=${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}
OUT_DIR=${OUT_DIR:-$REPO_DIR/fabperf/results/$RUN_ID}

cd "$REPO_DIR"
mkdir -p "$OUT_DIR" "$DATA_TMP"

# --- control variables --------------------------------------------------------
# Expand a sysfs range list ("0-3,5") into one CPU id per line.
expand_cpus() {
  local spec=$1 part lo hi c
  IFS=',' read -ra PARTS <<< "$spec"
  for part in "${PARTS[@]}"; do
    if [[ "$part" == *-* ]]; then
      lo=${part%-*}; hi=${part#*-}
      for ((c=lo; c<=hi; c++)); do echo "$c"; done
    else
      echo "$part"
    fi
  done
}

# Bring every present CPU online before reading anything. A previous run or a
# manual probe that left a sibling offline shrinks that core's topology file, so
# this script could no longer tell "this CPU has no sibling" from "its sibling is
# offline", and would record a degraded machine as if it were the setup.
PRESENT=$(cat /sys/devices/system/cpu/present 2>/dev/null || echo "")
SUDO_OK=0
if sudo -n true 2>/dev/null; then
  SUDO_OK=1
  if [[ -n "$PRESENT" ]]; then
    for c in $(expand_cpus "$PRESENT"); do
      state_file="/sys/devices/system/cpu/cpu${c}/online"
      [[ -f "$state_file" && "$(cat "$state_file")" == "0" ]] \
        && echo 1 | sudo tee "$state_file" >/dev/null 2>&1 || true
    done
  fi
fi

# Captured before the SMT sibling goes offline: once it does, the topology files
# shrink and the snapshot would describe the degraded machine, not the setup.
SIBLINGS=$(cat "/sys/devices/system/cpu/cpu${CORE}/topology/thread_siblings_list")
NPROC_TOTAL=$(nproc)
NPROC_PRESENT=$(echo "$PRESENT" | tr -d '\n')
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
if [[ -n "$SIBLING" ]] && [[ "$SUDO_OK" == "1" ]]; then
  echo 0 | sudo tee "/sys/devices/system/cpu/cpu${SIBLING}/online" >/dev/null
  trap restore_sibling EXIT
  echo "smt sibling cpu${SIBLING} offline for the run"
elif [[ -n "$SIBLING" ]]; then
  echo "WARNING: SMT sibling cpu${SIBLING} left ONLINE (no passwordless sudo)" >&2
else
  echo "note: cpu${CORE} reports no SMT sibling at its present topology"
fi
SIBLING_STATE="not_applicable"
[[ -z "$SIBLING" ]] && SIBLING_STATE="absent"
[[ -n "$SIBLING" && "$SUDO_OK" == "1" ]] && SIBLING_STATE="offlined_by_runner"
[[ -n "$SIBLING" && "$SUDO_OK" == "0" ]] && SIBLING_STATE="left_online_no_sudo"

snapshot_env() {
  python3 fabperf/env_snapshot.py --out "$OUT_DIR/env.json" --core "$CORE" \
    --flink-version "$FLINK_VERSION" --jar "${JAR:-pending}" \
    --siblings "$SIBLINGS" --nproc-total "$NPROC_TOTAL" \
    --present "$NPROC_PRESENT" --sibling-state "$SIBLING_STATE" \
    --warmup "$WARMUP" --iterations "$ITERATIONS" --iter-time "$ITER_TIME" \
    --xmx "$XMX" --run-id "$RUN_ID" \
    --note "Stage-1 single-core throughput baseline; groups: $BENCH_GROUPS"
}
snapshot_env

if [[ "$ENV_ONLY" == "1" ]]; then
  echo "ENV_ONLY=1 - snapshot written, no benchmark executed"
  exit 0
fi

# --- effective clock ----------------------------------------------------------
# The box exposes no usable PMU, so proposal section 3.3's cycles-per-record metric cannot be
# counted. A dependent ALU chain of two self-validating lengths estimates the clock the benchmark
# actually runs at; ingest uses it to convert ns/record into cycles/record, labelled as an estimate.
calibrate_clock() {
  local classes=/tmp/clockcalib
  mkdir -p "$classes"
  javac -d "$classes" fabperf/java/ClockCalib.java
  local seconds=${CALIB_SECONDS:-10}
  # Run on the same core, with the SMT sibling in the same state as the benchmark runs.
  taskset -c "$CORE" java -XX:ActiveProcessorCount=1 -cp "$classes" ClockCalib "$seconds" \
    > "$OUT_DIR/clockcalib.txt" 2>&1
  cat "$OUT_DIR/clockcalib.txt"
}

# --- jar ----------------------------------------------------------------------
find_jar() {
  # The shade plugin emits target/benchmarks.jar; older builds used the
  # flink-benchmarks-*.jar name, so accept either.
  if [[ -f target/benchmarks.jar ]]; then
    echo target/benchmarks.jar
  else
    ls target/flink-benchmarks-*.jar 2>/dev/null | grep -v original | head -1 || true
  fi
}
if [[ -z "$JAR" ]]; then
  JAR=$(find_jar)
  if [[ -z "$JAR" ]]; then
    echo "building benchmarks jar against flink $FLINK_VERSION ..."
    ./mvnw -q -DskipTests -Dflink.version="$FLINK_VERSION" -Drat.skip=true package
    JAR=$(find_jar)
  fi
fi
[[ -f "$JAR" ]] || { echo "jar not found: $JAR" >&2; exit 1; }
echo "jar: $JAR"
snapshot_env

if [[ "$CLOCK_CALIB" == "1" ]]; then
  calibrate_clock
fi

# --- groups -------------------------------------------------------------------
run_group() {
  local name=$1 pattern=$2
  echo "=== $name ($(date -u +%H:%M:%SZ)) ==="
  taskset -c "$CORE" java -cp "$JAR" org.openjdk.jmh.Main \
    -foe true -f 1 -wi "$WARMUP" -i "$ITERATIONS" -w "$ITER_TIME" -r "$ITER_TIME" \
    -jvmArgs "-Xmx$XMX -XX:ActiveProcessorCount=1 -Djava.io.tmpdir=$DATA_TMP" \
    -rf csv -rff "$OUT_DIR/$name.csv" \
    "$pattern" > "$OUT_DIR/$name.log" 2>&1 || {
    echo "FAILED: $name" >&2
    tail -20 "$OUT_DIR/$name.log" >&2
    exit 1
  }
  tail -4 "$OUT_DIR/$name.log"
}

for group in $BENCH_GROUPS; do
  case "$group" in
    window)       run_group window "org.apache.flink.benchmark.WindowBenchmarks" ;;
    state-memory) run_group state-memory "org.apache.flink.benchmark.MemoryStateBackendBenchmark" ;;
    state-rocks)  run_group state-rocks "org.apache.flink.benchmark.RocksStateBackendBenchmark" ;;
    cardinality)  run_group cardinality "org.apache.flink.benchmark.KeyedStateCardinalityBenchmark" ;;
    *) echo "unknown group: $group" >&2; exit 1 ;;
  esac
done

echo "RUN_ID=$RUN_ID results in $OUT_DIR"
