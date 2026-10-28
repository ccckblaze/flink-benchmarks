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
All parsing happens here rather than in shell awk to keep the quoting honest.
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


def lscpu_field(name):
    text = sh("LC_ALL=C lscpu")
    for line in text.splitlines():
        if line.lower().startswith(name.lower() + ":"):
            return line.split(":", 1)[1].strip()
    return ""


def cpu_model():
    return lscpu_field("Model name") or platform.processor()


def smt_state(core):
    siblings = rd("/sys/devices/system/cpu/cpu%d/topology/thread_siblings_list" % core)
    online = []
    for part in re.split(r"[,-]", siblings):
        if part.isdigit():
            online.append(rd("/sys/devices/system/cpu/cpu%s/online" % part) or "?")
    return siblings, online


def free_memory():
    text = sh("LC_ALL=C free -m")
    for line in text.splitlines():
        cols = line.split()
        if cols and cols[0].lower().startswith("mem:"):
            try:
                return "%s MB free / %s MB total" % (cols[3], cols[1])
            except IndexError:
                return line
    return ""


def allocator():
    java = sh("command -v java")
    if not java:
        return "n/a"
    real = sh("readlink -f %s" % java)
    deps = sh("ldd %s" % real)
    for name in ("libjemalloc", "libmimalloc", "libc.so"):
        match = re.search(name + r"\S*", deps)
        if match:
            return match.group(0)
    return "libc (no interposer found)"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--core", required=True)
    ap.add_argument("--flink-version", default="")
    ap.add_argument("--jar", default="")
    ap.add_argument("--warmup", default="")
    ap.add_argument("--iterations", default="")
    ap.add_argument("--iter-time", default="")
    ap.add_argument("--xmx", default="")
    ap.add_argument("--run-id", default="")
    ap.add_argument("--note", default="")
    args = ap.parse_args()

    siblings, sibling_online = smt_state(args.core)
    snap = {
        "run_id": args.run_id,
        "host": platform.node(),
        "date_utc": sh("date -u +%Y-%m-%dT%H:%M:%SZ"),
        "os": "%s %s" % (platform.system(), platform.release()),
        "cpu_model": cpu_model(),
        "nproc": sh("nproc"),
        "core_pinned": args.core,
        "core_siblings": siblings,
        "sibling_online": sibling_online,
        "governor": rd("/sys/devices/system/cpu/cpu%s/cpufreq/scaling_governor" % args.core) or "n/a",
        "cmdline": rd("/proc/cmdline"),
        "thp": rd("/sys/kernel/mm/transparent_hugepage/enabled"),
        "mem_free": free_memory(),
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
