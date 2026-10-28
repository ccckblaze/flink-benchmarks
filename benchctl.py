#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""benchctl — flinkable benchmark control on the flink-benchmarks methodology.

Upstream flow: benchmark.sh (JMH) -> jmh-result.csv -> Codespeed server ->
regression_report_v2.py. This tool keeps the discipline (single-machine
micro suites, direction-aware scores, baseline-window regression detection)
but replaces the server and the two Python-2 scripts with one local sqlite
store and Python-3 commands, and adds the native ingestion + report/chart
pair the JVM-only upstream never needed:

  ingest-jmh   a JMH `-rf csv` result file (upstream `benchmark.sh` output)
  ingest-measured  a flinkable rig log with MEASURED / P57-* lines
  put          one manually registered number (source-tagged)
  report       report.md + report.png from the store
  check        regression scan over stored history
  runs         list ingested runs
"""

import argparse
import sys

from flinkbench import ingest, regression, report, store

DEFAULT_DB = "flinkbench/bench.db"


def main(argv=None):
    ap = argparse.ArgumentParser(prog="benchctl.py", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=DEFAULT_DB)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("ingest-jmh", help="ingest a JMH -rf csv result file")
    p.add_argument("path")
    p.add_argument("--host", default="local")
    p.add_argument("--source", required=True, help="free-form provenance tag")

    p = sub.add_parser("ingest-measured", help="ingest a flinkable rig MEASURED log")
    p.add_argument("path")
    p.add_argument("--host", default="rig")
    p.add_argument("--source", required=True)

    p = sub.add_parser("put", help="register a single value")
    p.add_argument("case")
    p.add_argument("metric")
    p.add_argument("value", type=float)
    p.add_argument("unit")
    p.add_argument("--direction", choices=["lower_is_better", "higher_is_better"],
                   default="lower_is_better")
    p.add_argument("--host", default="local")
    p.add_argument("--source", required=True)

    p = sub.add_parser("gate-jvm", help="check a canonical log's JVM legs against stored history")
    p.add_argument("path")
    p.add_argument("--tolerance", type=float, default=0.15,
                   help="max allowed deviation from the stored median (default 0.15)")
    p.add_argument("--exclude-source", default=None,
                   help="run source to drop from the band (the session under review)")

    p = sub.add_parser("report", help="write report.md + report.png")
    p.add_argument("--out", default="flinkbench/out")
    p.add_argument("--title", default="flinkable × Apache Flink 关键指标同屏对比（benchctl 生成）")
    p.add_argument("--env", action="append", default=[],
                   help="environment line embedded in the report")

    p = sub.add_parser("check", help="regression scan")
    p.add_argument("--min-ratio", type=float, default=0.05)
    p.add_argument("--instability-multiplier", type=float, default=2.0)

    sub.add_parser("runs", help="list ingested runs")

    args = ap.parse_args(argv)
    con = store.connect(args.db)

    if args.cmd == "ingest-jmh":
        with open(args.path) as f:
            run = store.add_run(con, args.host, "jmh", args.source)
            n = ingest.ingest_jmh_csv(con, run, f.read())
        print("run %d: %d benchmarks stored" % (run, n))
    elif args.cmd == "ingest-measured":
        with open(args.path) as f:
            run = store.add_run(con, args.host, "native", args.source)
            n = ingest.ingest_measured_log(con, run, f.read())
        print("run %d: %d metrics stored" % (run, n))
    elif args.cmd == "put":
        run = store.add_run(con, args.host, "manual", args.source)
        store.add_metric(con, run, args.case, args.metric, args.value, args.unit, args.direction)
        print("run %d: %s/%s = %g %s stored" % (run, args.case, args.metric, args.value, args.unit))
    elif args.cmd == "gate-jvm":
        from flinkbench import gate
        with open(args.path) as f:
            ok, rows = gate.check(con, f.read(), tolerance=args.tolerance,
                                 exclude_source=args.exclude_source)
        for case, value, med, dev, verdict in rows:
            if med is None:
                print("%-24s session=%-7g %s" % (case, value, verdict))
            else:
                print("%-24s session=%-7g median=%-7g dev=%+.1f%% %s" % (
                    case, value, med, dev * 100, verdict))
        print("gate-jvm: %s" % ("PASS — 可入册锚定" if ok else "FAIL — JVM 腿出带，勿入册（M137 先例）"))
        sys.exit(0 if ok else 1)
    elif args.cmd == "report":
        env = args.env or ["环境：见 flinkbench/README.md（口径随 run 记录在库）"]
        print(report.generate(con, args.out, args.title, env))
    elif args.cmd == "check":
        bad = 0
        for case, metric, status, detail in regression.scan_store(
                con, min_ratio=args.min_ratio,
                instability_multiplier=args.instability_multiplier):
            if status == "regression":
                bad += 1
                print("REGRESSION %s/%s baseline=%s current=%s ratio=%.3f threshold=%.3f" % (
                    case, metric, detail["baseline_best"], detail["current_best"],
                    detail["regression_ratio"], detail["threshold"]))
        print("check done: %d regressions" % bad)
        sys.exit(1 if bad else 0)
    elif args.cmd == "runs":
        for rid, ts, host, suite, source in con.execute(
                "SELECT id, ts, host, suite, source FROM runs ORDER BY id"):
            print("%4d  %s  %-6s %-8s %s" % (rid, ts, host, suite, source))
    con.commit()


if __name__ == "__main__":
    main()
