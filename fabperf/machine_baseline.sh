#!/usr/bin/env bash
# Machine profile + baseline I/O collector for the measurement rig.
#
# The rig ships without fio/sysbench/mlc, so every probe here uses only
# coreutils (dd), lscpu/lsblk, and the small C probe in fabperf/c.  Large
# data writes go to the data disk (/mnt/datadisk0) per the standing rule;
# the root disk is measured with a smaller footprint for contrast only.
#
# Usage: fabperf/machine_baseline.sh [--out DIR] [--seq-mib N] [--root-seq-mib N]
set -euo pipefail
cd "$(dirname "$0")/.."

OUT_DIR="fabperf/results/$(date -u +%Y%m%dT%H%M%S)Z_machine"
SEQ_MIB=2048          # data disk probe file
ROOT_SEQ_MIB=1024     # root disk probe file (12 G free, keep it small)
DATA_DIR=/mnt/datadisk0
ROOT_DIR=/var/tmp
MEM_MB=512
MEM_SECONDS=3
RAND_SECONDS=10
FSYNC_COUNT=400

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) OUT_DIR=$2; shift 2 ;;
    --seq-mib) SEQ_MIB=$2; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done
mkdir -p "$OUT_DIR"
TXT="$OUT_DIR/machine_baseline.txt"
JSON="$OUT_DIR/machine_baseline.json"
PROBE_BIN=$(mktemp -t machine_probe.XXXXXX)
: > "$TXT"

emit() { echo "$1=$2" >> "$TXT"; }

# --- build the C probe --------------------------------------------------------
CC_BIN=""
for candidate in cc gcc clang; do
  if command -v "$candidate" >/dev/null 2>&1; then CC_BIN=$candidate; break; fi
done
if [[ -n "$CC_BIN" ]]; then
  "$CC_BIN" -O2 -o "$PROBE_BIN" fabperf/c/machine_probe.c
  emit probe_build "$CC_BIN -O2"
else
  echo "WARNING: no C compiler; disk randread/fsync and memory bandwidth skipped" >&2
  emit probe_build "none"
fi

# --- hardware profile ---------------------------------------------------------
emit generated_utc "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
emit host "$(uname -n)"
emit os "$(uname -srm)"
LSCPU=$(LC_ALL=C lscpu)
emit cpu_model "$(echo "$LSCPU" | awk -F: 'tolower($1) ~ /model name/ {gsub(/^ +/,"",$2); print $2; exit}')"
emit cpu_arch "$(echo "$LSCPU" | awk -F: 'tolower($1) ~ /architecture/ {gsub(/^ +/,"",$2); print $2; exit}')"
emit hypervisor "$(echo "$LSCPU" | awk -F: 'tolower($1) ~ /hypervisor vendor/ {gsub(/^ +/,"",$2); print $2; exit}')"
emit virt_type "$(echo "$LSCPU" | awk -F: 'tolower($1) ~ /virtualization type/ {gsub(/^ +/,"",$2); print $2; exit}')"
emit sockets "$(echo "$LSCPU" | awk -F: 'tolower($1) ~ /^socket/ {gsub(/^ +/,"",$2); print $2; exit}')"
emit cores_per_socket "$(echo "$LSCPU" | awk -F: 'tolower($1) ~ /core(s) per socket/ {gsub(/^ +/,"",$2); print $2; exit}')"
emit threads_per_core "$(echo "$LSCPU" | awk -F: 'tolower($1) ~ /thread/ {gsub(/^ +/,"",$2); print $2; exit}')"
emit nproc "$(nproc)"
emit cpu_present "$(cat /sys/devices/system/cpu/present 2>/dev/null || true)"
for cache in L1d L1i L2 L3; do
  emit "cache_$cache" "$(echo "$LSCPU" | awk -F: -v c="$cache" '$1 ~ c" cache" {gsub(/^ +/,"",$2); print $2; exit}')"
done
FLAGS=$(echo "$LSCPU" | awk -F: 'tolower($1) ~ /flags/ {print $2}')
for f in avx2 avx512f fma sha_ni aes; do
  if [[ " $FLAGS " == *" $f "* ]]; then emit "flag_$f" yes; else emit "flag_$f" no; fi
done
emit mem_total_mb "$(awk '/MemTotal/ {print $2/1024; exit}' /proc/meminfo)"
emit mem_available_mb "$(awk '/MemAvailable/ {print $2/1024; exit}' /proc/meminfo)"
emit thp "$(cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || true)"
# lsblk table -> one line per block device, fields joined by ';'
lsblk -d -b -o NAME,SIZE,TYPE,TRAN,ROTA,MODEL 2>/dev/null | tail -n +2 |
  while read -r line; do echo "disk_row|$line" >> "$TXT"; done
df -B1 -T 2>/dev/null | tail -n +2 | grep -vE 'tmpfs|devtmpfs|overlay|proc|sys' |
  while read -r line; do echo "fs_row|$line" >> "$TXT"; done
NIC=""
for n in /sys/class/net/*; do
  name=$(basename "$n")
  [[ "$name" == "lo" ]] && continue
  speed=$(cat "$n/speed" 2>/dev/null || echo -1)
  driver=$(cat "$n/device/driver" 2>/dev/null | xargs -I{} basename {} || true)
  NIC="$NIC;$name=speed:${speed:-unknown},driver:${driver:-unknown}"
done
emit nic "${NIC#;}"

# --- disk baselines ------------------------------------------------------------
parse_dd_rate() { # dd final line -> MiB/s (decimal/binary prefixes normalised)
  local line=$1 rate unit mult
  rate=$(echo "$line" | sed -n 's/.*copied, [0-9.]* s, \([0-9.]*\) \([A-Za-z]*\)\/s/\1/p' | tail -1)
  unit=$(echo "$line" | sed -n 's/.*copied, [0-9.]* s, [0-9.]* \([A-Za-z]*\)\/s/\1/p' | tail -1)
  [[ -z "$rate" ]] && { echo "0"; return; }
  mult=1
  case "$unit" in
    GB) mult=1000 ;;
    GiB) mult=1024 ;;
    MB|MiB) mult=1 ;;
    kB|K) mult=0.001 ;;
    KiB) mult=0.0009765625 ;;
    B) mult=0.000000953674 ;;
  esac
  awk -v r="$rate" -v m="$mult" 'BEGIN { printf "%.1f", r * m }'
}

dd_probe() { # tag dir mib mode(write|read)
  local tag=$1 dir=$2 mib=$3 mode=$4 out rc line
  local file="$dir/machine_probe_seq"
  rc=0
  if [[ "$mode" == write ]]; then
    # oflag=direct only proves the guest can hand bytes to virtio; the host
    # backend keeps them in its own cache, so this leg is labelled, not headline.
    out=$(LC_ALL=C dd if=/dev/zero of="$file" bs=1M count="$mib" oflag=direct 2>&1) || rc=1
    if [[ $rc -eq 0 && "$out" == *"copied"* ]]; then
      emit "disk_${tag}_hostcache_dd_line" "$(echo "$out" | tail -1)"
      emit "disk_${tag}_write_hostcache_mibps" "$(parse_dd_rate "$(echo "$out" | tail -1)")"
    else
      emit "disk_${tag}_write_direct_error" "$(echo "$out" | head -1 | cut -c1-120)"
    fi
    # The headline write number must reach the persistent backend: fdatasync
    # flushes through virtio to the cloud disk.
    out=$(LC_ALL=C dd if=/dev/zero of="$file" bs=1M count="$mib" conv=fdatasync 2>&1) || rc=1
    emit "disk_${tag}_write_mode" "fdatasync_persisted"
    line=$(echo "$out" | tail -1)
    emit "disk_${tag}_write_dd_line" "$line"
    emit "disk_${tag}_seq_write_mibps" "$(parse_dd_rate "$line")"
    return
  fi
  sync
  if command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null 2>&1 &&
      emit "disk_${tag}_cache_dropped" "yes" || emit "disk_${tag}_cache_dropped" "no"
  else
    emit "disk_${tag}_cache_dropped" "no"
  fi
  rc=0
  out=$(LC_ALL=C dd if="$file" of=/dev/null bs=1M iflag=direct 2>&1) || rc=1
  if [[ $rc -ne 0 || "$out" != *"copied"* ]]; then
    emit "disk_${tag}_read_direct_error" "$(echo "$out" | head -1 | cut -c1-120)"
    out=$(LC_ALL=C dd if="$file" of=/dev/null bs=1M 2>&1)
    emit "disk_${tag}_read_mode" "cached_fallback"
  else
    emit "disk_${tag}_read_mode" "direct"
  fi
  line=$(echo "$out" | tail -1)
  emit "disk_${tag}_read_dd_line" "$line"
  emit "disk_${tag}_seq_read_mibps" "$(parse_dd_rate "$line")"
}

if [[ -d "$DATA_DIR" ]]; then
  dd_probe datadisk "$DATA_DIR" "$SEQ_MIB" write
  dd_probe datadisk "$DATA_DIR" "$SEQ_MIB" read
  emit disk_datadisk_seq_mib "$SEQ_MIB"
else
  echo "WARNING: $DATA_DIR missing; data-disk legs skipped" >&2
fi
dd_probe root "$ROOT_DIR" "$ROOT_SEQ_MIB" write
dd_probe root "$ROOT_DIR" "$ROOT_SEQ_MIB" read
emit disk_root_seq_mib "$ROOT_SEQ_MIB"

if [[ -n "$CC_BIN" ]]; then
  if [[ -d "$DATA_DIR" && -f "$DATA_DIR/machine_probe_seq" ]]; then
    "$PROBE_BIN" randread "$DATA_DIR/machine_probe_seq" "$RAND_SECONDS" |
      sed 's/^/disk_datadisk_/' >> "$TXT"
  fi
  if [[ -d "$DATA_DIR" ]]; then
    "$PROBE_BIN" fsync "$DATA_DIR/machine_probe_fsync" 4096 "$FSYNC_COUNT" |
      sed 's/^/disk_datadisk_/' >> "$TXT"
  fi
  "$PROBE_BIN" fsync "$ROOT_DIR/machine_probe_fsync" 4096 "$FSYNC_COUNT" |
    sed 's/^/disk_root_/' >> "$TXT"
  "$PROBE_BIN" membw copy "$MEM_MB" "$MEM_SECONDS" 1 | sed 's/^membw_/mem_copy1_/' >> "$TXT"
  "$PROBE_BIN" membw triad "$MEM_MB" "$MEM_SECONDS" 1 | sed 's/^membw_/mem_triad1_/' >> "$TXT"
  NTHREADS=$(nproc)
  "$PROBE_BIN" membw copy "$MEM_MB" "$MEM_SECONDS" "$NTHREADS" | sed 's/^membw_/mem_copyall_/' >> "$TXT"
  emit mem_buffer_mb_per_stream "$MEM_MB"
  emit mem_seconds "$MEM_SECONDS"
  emit mem_threads_all "$NTHREADS"
fi

rm -f "$DATA_DIR/machine_probe_seq" "$DATA_DIR/machine_probe_fsync" \
      "$ROOT_DIR/machine_probe_seq" "$ROOT_DIR/machine_probe_fsync" \
      /tmp/var_tmp_probe_fsync.* 2>/dev/null || true
rm -f "$PROBE_BIN"

# --- assemble JSON -------------------------------------------------------------
python3 - "$TXT" "$JSON" <<'PY'
import json, sys

rows = {}
for line in open(sys.argv[1]):
    line = line.strip()
    if "|" in line and not line.startswith("disk_row|") and not line.startswith("fs_row|"):
        continue
    if line.startswith("disk_row|"):
        rows.setdefault("disks", []).append(line.split("|", 1)[1].split())
        continue
    if line.startswith("fs_row|"):
        rows.setdefault("fs", []).append(line.split("|", 1)[1].split())
        continue
    if "=" in line:
        k, v = line.split("=", 1)
        rows[k] = v

def num(v):
    try:
        f = float(v)
        return int(f) if f.is_integer() else f
    except (TypeError, ValueError):
        return v or None

disk = {}
for tag in ("datadisk", "root"):
    sub = {k[len("disk_%s_" % tag):]: v for k, v in
           ((k, rows.get(k)) for k in rows if k.startswith("disk_%s_" % tag)) if v is not None}
    if sub:
        disk[tag] = {kk: num(vv) for kk, vv in sub.items()}

hardware = {
    "model": rows.get("cpu_model"), "arch": rows.get("cpu_arch"),
    "hypervisor": rows.get("hypervisor"), "virtualization_type": rows.get("virt_type"),
    "sockets": rows.get("sockets"), "cores_per_socket": rows.get("cores_per_socket"),
    "threads_per_core": rows.get("threads_per_core"), "nproc": rows.get("nproc"),
    "cpu_present": rows.get("cpu_present"), "mem_total_mb": num(rows.get("mem_total_mb")),
    "mem_available_mb": num(rows.get("mem_available_mb")), "thp": rows.get("thp"),
    "nic": rows.get("nic"), "os": rows.get("os"),
    "caches": {k[6:]: rows[k] for k in rows if k.startswith("cache_")},
    "flags": {k[5:]: rows[k] for k in rows if k.startswith("flag_")},
    "block_devices": [
        {"name": r[0], "size_bytes": num(r[1]) if len(r) > 1 else None,
         "type": r[2] if len(r) > 2 else None, "transport": r[3] if len(r) > 3 else None,
         "rotational_flag": r[4] if len(r) > 4 else None,
         "model": " ".join(r[5:]) if len(r) > 5 and r[5] else None}
        for r in rows.get("disks", []) if r],
    "filesystems": [
        {"device": r[0], "fstype": r[1], "size_bytes": num(r[2]),
         "used_bytes": num(r[3]), "avail_bytes": num(r[4]), "mount": r[5]}
        for r in rows.get("fs", []) if len(r) >= 6],
}
memory = {k[4:]: num(v) for k, v in rows.items() if k.startswith("mem_")}
out = {"generated_utc": rows.get("generated_utc"), "host": rows.get("host"),
       "hardware": hardware, "disk": disk, "memory": memory}
json.dump(out, open(sys.argv[2], "w"), ensure_ascii=False, indent=1)
print("machine baseline ->", sys.argv[2])
PY
cat "$TXT"
