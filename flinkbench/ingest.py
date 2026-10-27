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
"""Two ingestion paths, one store.

* `ingest_jmh_csv` reads the standard `jmh-result.csv` that upstream's
  `benchmark.sh` produces with `-rf csv` — header-driven, `Param,*` columns fold
  into the case name exactly like upstream's `save_jmh_result.py` does, and the
  JMH mode decides the direction (thrpt: more is better; avgt/ss: less is
  better), same semantics as upstream.
* `ingest_measured_log` reads the native rig's MEASURED / P57-* lines, the
  flinkable counterpart of a JMH result file.
"""

import csv
import io
import re

LB = "lower_is_better"
HB = "higher_is_better"


def ingest_jmh_csv(con, run_id, text):
    # Header-index based like upstream save_jmh_result.py: JMH repeats the
    # literal column name "Param" once per parameter, so DictReader would
    # collapse them.
    reader = csv.reader(io.StringIO(text))
    header = next(reader, None) or []
    if "Benchmark" not in header or "Score" not in header:
        raise ValueError("not a JMH csv: missing Benchmark/Score columns")
    bi, mi = header.index("Benchmark"), header.index("Mode")
    si, ui = header.index("Score"), header.index("Unit") if "Unit" in header else None
    ei = header.index("Score Error (99.9999%)") if "Score Error (99.9999%)" in header else None
    pidx = [i for i, h in enumerate(header) if h.startswith("Param")]
    count = 0
    for row in reader:
        if not row or not row[bi].strip():
            continue
        case = row[bi].split(".")[-1]
        for i in pidx:
            if row[i].strip():
                case += "." + row[i].strip()
        direction = LB if row[mi].strip() in ("avgt", "ss", "sample_size") else HB
        unit = row[ui].strip() if ui is not None else ""
        con.execute(
            "INSERT INTO metrics (run_id, case_name, metric, value, unit, direction) VALUES (?, ?, 'score', ?, ?, ?)",
            (run_id, case, float(row[si]), unit, direction),
        )
        if ei is not None and row[ei].strip():
            con.execute(
                "INSERT INTO metrics (run_id, case_name, metric, value, unit, direction) VALUES (?, ?, 'error', ?, ?, ?)",
                (run_id, case, float(row[ei]), unit, direction),
            )
        count += 1
    return count


def _case(name):
    return name.replace("+", "_").replace(" ", "_").replace("-", "_")


MEASURED_PATTERNS = [
    # fab-arrow data_plane_throughput.rs
    (r"MEASURED encode: .*-> ([\d.]+) MiB/s, ([\d.]+) ns/row",
     lambda m: [("framing_encode", "throughput", m[0], "MiB/s", HB),
                ("framing_encode", "per_record", m[1], "ns/row", LB)]),
    (r"MEASURED decode: .*-> ([\d.]+) MiB/s, ([\d.]+) ns/row, ([\d.]+) ns/cell",
     lambda m: [("framing_decode", "throughput", m[0], "MiB/s", HB),
                ("framing_decode", "per_record", m[1], "ns/row", LB),
                ("framing_decode", "per_cell", m[2], "ns/cell", LB)]),
    (r"MEASURED wire: ([\d.]+) bytes/row",
     lambda m: [("framing_wire", "bytes_per_record", m[0], "B/row", LB)]),
    (r"MEASURED take: .*-> ([\d.]+) Mrows/s, ([\d.]+) ns/row \(4 cols: ([\d.]+) ns/cell\)",
     lambda m: [("shuffle_take", "throughput", m[0], "Mrec/s", HB),
                ("shuffle_take", "per_record", m[1], "ns/row", LB),
                ("shuffle_take", "per_cell", m[2], "ns/cell", LB)]),
    # fab-embed embed_mixed.rs / embed_bench.rs interface segments
    (r"export-only \(Rust struct build\)\s+([\d.]+) ns/row\s+(\d+) MiB/s",
     lambda m: [("embed_interface", "export_only", m[0], "ns/row", LB),
                ("embed_interface", "export_throughput", m[1], "MiB/s", HB)]),
    (r"L2 wrap-only \(import, no touch\)\s+([\d.]+) ns/row\s+(\d+) MiB/s",
     lambda m: [("embed_interface", "l2_zero_copy", m[0], "ns/row", LB),
                ("embed_interface", "l2_throughput", m[1], "MiB/s", HB)]),
    (r"L1 transfer \(IPC encode\+decode\)\s+([\d.]+) ns/row\s+(\d+) MiB/s",
     lambda m: [("embed_interface", "l1_ipc", m[0], "ns/row", LB),
                ("embed_interface", "l1_throughput", m[1], "MiB/s", HB)]),
    (r"ratio L1/L2 transfer segment: ([\d.]+)x",
     lambda m: [("embed_interface", "transfer_ratio", m[0], "x", HB)]),
    (r"L2 full \(wrap \+ pandas touch\)\s+([\d.]+) ns/row",
     lambda m: [("compute_pandas", "l2_end_to_end", m[0], "ns/row", LB)]),
    (r"L1 full \(IPC \+ pandas touch\)\s+([\d.]+) ns/row",
     lambda m: [("compute_pandas", "l1_end_to_end", m[0], "ns/row", LB)]),
    (r"ratio L1/L2 full: ([\d.]+)x \| touch share",
     lambda m: [("compute_pandas", "end_to_end_ratio", m[0], "x", HB)]),
    (r"L2 full \(wrap \+ compute touch\)\s+([\d.]+) ns/row",
     lambda m: [("compute_arrow_native", "l2_end_to_end", m[0], "ns/row", LB)]),
    (r"L1 full \(IPC \+ compute touch\)\s+([\d.]+) ns/row",
     lambda m: [("compute_arrow_native", "l1_end_to_end", m[0], "ns/row", LB)]),
    (r"ratio L1/L2 full: ([\d.]+)x",
     lambda m: [("compute_arrow_native", "end_to_end_ratio", m[0], "x", HB)]),
    # M98 small-batch curve
    (r"M98 curve \| batch=\s*(\d+) .*L2 wrap\s+\d+ ns/batch\s+([\d.]+) ns/row \| L1 IPC\s+\d+ ns/batch\s+([\d.]+) ns/row \| L1/L2 transfer ratio ([\d.]+)x",
     lambda m: [("smallbatch_" + m[0], "l2_zero_copy", m[1], "ns/row", LB),
                ("smallbatch_" + m[0], "l1_ipc", m[2], "ns/row", LB),
                ("smallbatch_" + m[0], "transfer_ratio", m[3], "x", HB)]),
    # JVM comparison table (M125/M126) and legacy owned-form serde (P57)
    (r"P57-TABLE\|([^|]+)\|rust=([\d.]+)\|jvm=([\d.]+)\|jvm_over_rust=([\d.]+)x",
     lambda m: [("serde_" + _case(m[0]), "flinkable", m[1], "ns/rec", LB),
                ("serde_" + _case(m[0]), "flink_jvm", m[2], "ns/rec", LB),
                ("serde_" + _case(m[0]), "flink_over_flinkable", m[3], "x", HB)]),
    # M128 same-session before/after: the M126 shapes priced beside the M128 lanes.
    (r"AB\|([^|]+)\|enc_old=([\d.]+)\|enc_new=([\d.]+)\|dec_old=([\d.]+)\|dec_new=([\d.]+)\|dec_cow=([\d.]+)",
     lambda m: [("ab_encode_" + m[0], "flinkable_before", m[1], "ns/rec", LB),
                ("ab_encode_" + m[0], "flinkable", m[2], "ns/rec", LB),
                ("ab_decode_" + m[0], "flinkable_before", m[3], "ns/rec", LB),
                ("ab_decode_" + m[0], "flinkable", m[4], "ns/rec", LB),
                ("ab_decode_" + m[0], "flinkable_borrow", m[5], "ns/rec", LB)]),
    (r"P57-SERDE int\+long encode/decode=([\d.]+) ns/rec",
     lambda m: [("serde_owned_form_int_long", "flinkable", m[0], "ns/rec", LB)]),
    (r"P57-SERDE string encode/decode=([\d.]+) ns/rec",
     lambda m: [("serde_owned_form_string", "flinkable", m[0], "ns/rec", LB)]),
    (r"P57-FUSE chained=([\d.]+) ns/rec fused=([\d.]+) ns/rec speedup=([\d.]+)x",
     lambda m: [("operator_fusion", "chained", m[0], "ns/rec", LB),
                ("operator_fusion", "fused", m[1], "ns/rec", LB),
                ("operator_fusion", "speedup", m[2], "x", HB)]),
    (r"P57-TAIL chained-map\+exchange=([\d.]+) ns/rec",
     lambda m: [("network_accounting", "per_record", m[0], "ns/rec", LB)]),
    (r"P57-HOP channel round-trip=([\d.]+) ns/rec",
     lambda m: [("cross_task_hop_floor", "per_record", m[0], "ns/rec", LB)]),
]

_COMPILED = [(re.compile(p), f) for p, f in MEASURED_PATTERNS]


def ingest_measured_log(con, run_id, text):
    count = 0
    for line in text.splitlines():
        for pattern, emit in _COMPILED:
            m = pattern.search(line)
            if m:
                for case, metric, value, unit, direction in emit([m.group(i) for i in range(1, m.lastindex + 1)]):
                    con.execute(
                        "INSERT INTO metrics (run_id, case_name, metric, value, unit, direction) VALUES (?, ?, ?, ?, ?, ?)",
                        (run_id, case, metric, float(value), unit, direction),
                    )
                    count += 1
                break
    return count
