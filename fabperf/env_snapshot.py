#!/usr/bin/env python3
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
"""Record the control variables of a measurement run beside its results.

PERF_EXPERIMENT_PROPOSAL.md requires every leg to carry CPU model, pinning,
governor, SMT state, THP, allocator, kernel cmdline, free memory and load, so
a later cross-engine comparison can prove the two legs saw the same machine.
Parsing lives here rather than in shell awk to keep the quoting honest.  The
sibling list and total core count must be passed in from the runner, captured
*before* any SMT core is taken offline, otherwise the snapshot would describe
the degraded machine rather than the configured one.
"""

import argparse
import json
import platform
import re
import subprocess
import sys


def sh(cmd):
    try:
        res = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=15)
    except Exception:
        return ""
    return (res.stdout or "").strip() or (res.stderr or "").strip()


def rd(path):
    try:
        with open(path) as handle:
            return handle.read().strip()
    except OSError:
        return ""


def lscpu():
    return sh("LC_ALL=C lscpu")


def lscpu_field(name, text=None):
    for line in (text if text is not None else lscpu()).splitlines():
        if line.lower().startswith(name.lower() + ":"):
            return line.split(":", 1)[1].strip()
    return ""


def cpu_model(cpuinfo_text=""):
    """Hypervisors often report a placeholder model; say so instead of lying."""
    candidates = [
        lscpu_field("Model name"),
        (lambda text: re.search(r"model name\s*:\s*(.+)", text).group(1).strip()
         if re.search(r"model name\s*:\s*(.+)", text) else "")(cpuinfo_text),
        platform.processor(),
    ]
    for candidate in candidates:
        if candidate and "general processor" not in candidate.lower():
            return candidate
    return "hypervisor hides the model (vCPU)"


def siblings_of(core):
    return rd("/sys/devices/system/cpu/cpu%s/topology/thread_siblings_list" % core)


def sibling_online(siblings):
    """Online flag per listed sibling; the boot cpu has no `online` file."""
    states = []
    for part in re.split(r"[,-]", siblings or ""):
        if not part.isdigit():
            continue
        value = rd("/sys/devices/system/cpu/cpu%s/online" % part)
        states.append("cpu%s=%s" % (part, value if value else "1 (boot cpu)"))
    return states


def free_memory():
    for line in sh("LC_ALL=C free -m").splitlines():
        cols = line.split()
        if cols and cols[0].lower().startswith("mem:"):
            try:
                return "%s MB available / %s MB free / %s MB total" % (
                    cols[6], cols[3], cols[1])
            except IndexError:
                return line
    return ""


def cpu_caches(lscpu_text=None):
    """Last-level cache size in bytes; the cardinality knee is read against it."""
    caches = {}
    for line in (lscpu_text if lscpu_text is not None else lscpu()).splitlines():
        match = re.match(r"(L\d[di]) cache:\s*(.+)", line.strip())
        if match:
            caches[match.group(1)] = match.group(2).strip()
    return caches


def cache_bytes(caches, name="L3"):
    """Parse `32 MiB (1 instance)` style values down to per-instance bytes."""
    value = caches.get(name, "")
    match = re.search(r"([\d.]+)\s*(KiB|MiB|GiB|K\b|M\b|G\b)", value)
    if not match:
        return None
    scale = {"KiB": 1024, "MiB": 1024 ** 2, "GiB": 1024 ** 3,
             "K": 1000, "M": 1000 ** 2, "G": 1000 ** 3}[match.group(2)]
    return int(float(match.group(1)) * scale)


def virtualization(lscpu_text=None):
    text = lscpu_text if lscpu_text is not None else lscpu()
    vendor = lscpu_field("Hypervisor vendor", text)
    kind = lscpu_field("Virtualization type", text)
    return ("%s / %s" % (vendor, kind)).strip(" /") or "none (bare metal)"


def block_devices():
    rows = sh("lsblk -d -b -o NAME,SIZE,TYPE,TRAN,ROTA,MODEL").splitlines()
    devices = []
    for line in rows[1:]:
        cols = line.split(None, 5)
        if len(cols) >= 5:
            devices.append({
                "name": cols[0], "size_bytes": cols[1], "type": cols[2],
                "transport": cols[3], "rotational_flag": cols[4],
                "model": cols[5] if len(cols) > 5 else ""})
    return devices


def filesystems():
    rows = sh("df -B1 -x tmpfs -x devtmpfs -x overlay").splitlines()
    out = []
    for line in rows[1:]:
        cols = line.split()
        if len(cols) >= 6:
            out.append({"device": cols[0], "size_bytes": cols[1],
                        "avail_bytes": cols[4], "mount": cols[5]})
    return out


def network():
    nics = []
    for path in sh("ls -d /sys/class/net/* 2>/dev/null").split():
        name = path.rsplit("/", 1)[-1]
        if name == "lo":
            continue
        speed = rd(path + "/speed")
        nics.append({"name": name, "speed_mbps": speed if speed and speed != "-1"
                     else "not exposed by hypervisor"})
    return nics


def allocator():
    java = sh("command -v java")
    if not java:
        return "n/a"
    deps = sh("ldd %s" % sh("readlink -f %s" % java))
    for name in ("libjemalloc", "libmimalloc", "libc.so"):
        match = re.search(name + r"\S*", deps)
        if match:
            return match.group(0)
    return "libc (no interposer found)"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--core", required=True)
    ap.add_argument("--siblings", default="")
    ap.add_argument("--nproc-total", default="")
    ap.add_argument("--present", default="",
                    help="static /sys/devices/system/cpu/present range, unaffected by hotplug")
    ap.add_argument("--sibling-state", default="",
                    help="offlined_by_runner | left_online_no_sudo | absent")
    ap.add_argument("--flink-version", default="")
    ap.add_argument("--jar", default="")
    ap.add_argument("--warmup", default="")
    ap.add_argument("--iterations", default="")
    ap.add_argument("--iter-time", default="")
    ap.add_argument("--xmx", default="")
    ap.add_argument("--run-id", default="")
    ap.add_argument("--note", default="")
    args = ap.parse_args()

    cpuinfo = rd("/proc/cpuinfo")
    siblings = args.siblings or siblings_of(args.core)
    lscpu_text = lscpu()
    snap = {
        "run_id": args.run_id,
        "host": platform.node(),
        "date_utc": sh("date -u +%Y-%m-%dT%H:%M:%SZ"),
        "os": "%s %s" % (platform.system(), platform.release()),
        "cpu_model": cpu_model(cpuinfo),
        "cpu_arch": lscpu_field("Architecture", lscpu_text),
        "nproc_total": args.nproc_total or sh("nproc"),
        # `present` is the honest machine size: nproc shrinks when a sibling is
        # offline, which previously made a run look like it happened on a
        # smaller box than it did.
        "cpu_present": args.present,
        "sibling_state": args.sibling_state,
        "nproc_online": sh("nproc"),
        "core_pinned": args.core,
        "core_siblings": siblings,
        "sibling_online": sibling_online(siblings),
        "governor": rd(
            "/sys/devices/system/cpu/cpu%s/cpufreq/scaling_governor" % args.core) or "n/a",
        "cstate_kernel_param": " ".join(
            word for word in rd("/proc/cmdline").split()
            if "cstate" in word or "pstate" in word),
        "cmdline": rd("/proc/cmdline"),
        "cpu_mhz": (re.search(r"cpu MHz\s*:\s*([\d.]+)", cpuinfo).group(1)
                    if re.search(r"cpu MHz\s*:\s*([\d.]+)", cpuinfo) else ""),
        "thp": rd("/sys/kernel/mm/transparent_hugepage/enabled"),
        "mem_free": free_memory(),
        "virtualization": virtualization(lscpu_text),
        "cpu_caches": cpu_caches(lscpu_text),
        "l3_bytes": cache_bytes(cpu_caches(lscpu_text)),
        "block_devices": block_devices(),
        "filesystems": filesystems(),
        "network": network(),
        "loadavg": rd("/proc/loadavg"),
        "uptime": sh("uptime -p"),
        "java": sh("java -version 2>&1"),
        "allocator": allocator(),
        "flink_version": args.flink_version,
        "jar": args.jar,
        "jmh": {
            "forks": 1,
            "warmup_iterations": args.warmup,
            "iterations": args.iterations,
            "iter_time": args.iter_time,
            "xmx": args.xmx,
            "active_processor_count": "1 (pinned)",
        },
        "note": args.note,
    }
    with open(args.out, "w") as handle:
        json.dump(snap, handle, ensure_ascii=False, indent=1)
        handle.write("\n")
    print("env snapshot -> %s" % args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
