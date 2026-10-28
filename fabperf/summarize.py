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
"""Turn a fabperf run directory into a readable baseline markdown report.

Reads the JMH csv files produced by flink_baseline.sh plus the env.json
control-variable snapshot, and writes baseline.md next to them.  The numbers
are the median (Score) over the measurement iterations with the JMH 99.9999%
error bar, exactly as JMH reports them - no re-averaging, so the table can be
diffed against a later Flinkable leg run on the same box.
"""

import argparse
import csv
import io
import json
import os
import sys


def parse_csv(path):
    with open(path, newline="") as handle:
        reader = csv.reader(io.StringIO(handle.read()))
        header = next(reader, None) or []
        if "Benchmark" not in header or "Score" not in header:
            return []
        idx = {name: i for i, name in enumerate(header)}
        pidx = [i for i, name in enumerate(header) if name.startswith("Param")]
        rows = []
        for row in reader:
            if not row or not row[idx["Benchmark"]].strip():
                continue
            bench = row[idx["Benchmark"]]
            short = bench.split(".")[-1]
            params = ",".join(row[i].strip() for i in pidx if row[i].strip())
            err = ""
            if "Score Error (99.9999%)" in idx and row[idx["Score Error (99.9999%)"]].strip():
                err = row[idx["Score Error (99.9999%)"]]
            rows.append({
                "case": short + ("." + params if params else ""),
                "mode": row[idx["Mode"]],
                "cnt": row[idx.get("Cnt", 0)] if "Cnt" in idx else "",
                "score": float(row[idx["Score"]]),
                "error": err,
                "unit": row[idx["Unit"]] if "Unit" in idx else "",
            })
        return rows


def fmt(value):
    if value >= 100000:
        return "%.3g" % value
    if value >= 100:
        return "%.0f" % value
    return "%.2f" % value


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir")
    ap.add_argument("--out", default="")
    ap.add_argument("--title", default="Flink Stage-1 baseline (flink-benchmarks JMH)")
    args = ap.parse_args()

    env = {}
    env_path = os.path.join(args.run_dir, "env.json")
    if os.path.exists(env_path):
        with open(env_path) as handle:
            env = json.load(handle)

    groups = []
    for name in sorted(os.listdir(args.run_dir)):
        if not name.endswith(".csv"):
            continue
        rows = parse_csv(os.path.join(args.run_dir, name))
        if rows:
            groups.append((name[:-4], rows))

    out = args.out or os.path.join(args.run_dir, "baseline.md")
    lines = ["# %s" % args.title, ""]
    if env:
        lines += ["## 测量环境（控制变量）", "", "| 项 | 值 |", "| --- | --- |"]
        order = [
            ("run_id", "Run ID"), ("host", "机器"), ("date_utc", "时间 (UTC)"),
            ("os", "系统"), ("cpu_model", "CPU"), ("cpu_arch", "架构"),
            ("nproc_total", "逻辑核数（配置）"), ("nproc_online", "逻辑核数（在线）"),
            ("core_pinned", "绑核"), ("core_siblings", "同物理核兄弟"),
            ("sibling_online", "兄弟核在线状态"), ("cpu_mhz", "主频"),
            ("governor", "调频策略"), ("cstate_kernel_param", "C-state 内核参数"),
            ("thp", "透明大页"), ("mem_free", "内存"), ("loadavg", "负载"),
            ("allocator", "内存分配器"), ("flink_version", "Flink 版本"),
            ("jar", "jar"), ("note", "说明"),
        ]
        for key, label in order:
            if env.get(key):
                value = env[key]
                if isinstance(value, list):
                    value = " ".join(str(v) for v in value)
                value = str(value).replace("\n", " / ")
                lines.append("| %s | %s |" % (label, value))
        jmh = env.get("jmh") or {}
        if jmh:
            cfg = "forks=%s, warmup=%s, iterations=%s, iter_time=%s, xmx=%s, ActiveProcessorCount=%s" % (
                jmh.get("forks"), jmh.get("warmup_iterations"), jmh.get("iterations"),
                jmh.get("iter_time"), jmh.get("xmx"), jmh.get("active_processor_count"))
            lines.append("| JMH 参数 | %s |" % cfg)
        java = (env.get("java") or "").splitlines()
        if java:
            lines.append("| Java | %s |" % java[0])
        if env.get("cmdline"):
            lines.append("| 内核命令行 | `%s` |" % env["cmdline"])
        lines.append("")

    lines += ["## 结果", ""]
    for group, rows in groups:
        lines += ["### %s" % group, "",
                  "| 用例 | 模式 | 迭代数 | 得分 | 误差 (99.9999%) | 单位 |",
                  "| --- | --- | --- | --- | --- | --- |"]
        for row in sorted(rows, key=lambda r: r["case"]):
            lines.append("| %s | %s | %s | %s | %s | %s |" % (
                row["case"], row["mode"], row["cnt"], fmt(row["score"]),
                row["error"] or "-", row["unit"]))
        lines.append("")

    with open(out, "w") as handle:
        handle.write("\n".join(lines))
    print("%s -> %s (%d groups, %d cases)" % (
        out, "ok", len(groups), sum(len(r) for _, r in groups)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
