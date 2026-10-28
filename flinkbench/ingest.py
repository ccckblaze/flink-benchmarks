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
    # M131: the incumbent encoder on the same columnar wire — PyFlink's ArrowCoder shape
    # (one self-contained IPC stream per batch), priced in the anchor Python.
    (r"MEASURED pyflink coder: encode ([\d.]+) ns/row, decode ([\d.]+) ns/row",
     lambda m: [("framing_coder", "pyflink_enc", m[0], "ns/rec", LB),
                ("framing_coder", "pyflink_dec", m[1], "ns/rec", LB),
                ("framing_coder", "pyflink", str(float(m[0]) + float(m[1])), "ns/rec", LB)]),
    # M131: standardized distributed-deployment RSS — one JobManager + one TaskManager,
    # WordCount over a 5.5 GB data-disk input, medians of twenty 3 s samples per process.
    (r"MEM\|(jm|tm)\|jvm=([\d.]+)\|flinkable=([\d.]+)",
     lambda m: [("memory_" + m[0] + "_rss", "flink_jvm", m[1], "MB", LB),
                ("memory_" + m[0] + "_rss", "flinkable", m[2], "MB", LB),
                ("memory_" + m[0] + "_rss", "flink_over_flinkable", str(float(m[1]) / float(m[2])), "x", HB)]),
    # M131: columnar framing vs JVM row serialization, same record shape, one session.
    (r"P58-TABLE\|bulk4\|rust_enc=([\d.]+)\|rust_dec=([\d.]+)\|rust=([\d.]+)\|(?:rust_count=([\d.]+)\|)?jvm=([\d.]+)\|jvm_over_rust=([\d.]+)x",
     lambda m: [("bulk4_framing", "flink_enc", m[0], "ns/rec", LB),
                ("bulk4_framing", "flink_dec", m[1], "ns/rec", LB),
                ("bulk4_framing", "flinkable", m[2], "ns/rec", LB)]
               + ([("serde_bulk4_count", "flinkable", m[3], "ns/rec", LB)] if len(m) == 6 else [])
               + [("bulk4_framing", "flink_jvm", m[3] if len(m) == 5 else m[4], "ns/rec", LB),
                  ("bulk4_framing", "flink_over_flinkable", m[4] if len(m) == 5 else m[5], "x", HB)]),
    # M129: both engines' string terms in one session — construction vs codec.
    (r"P57-TABLE\|string-terms\|rust_construct=([\d.]+)\|rust_codec=([\d.]+)\|jvm_construct=([\d.]+)\|jvm_codec=([\d.]+)\|construct_over_rust=([\d.]+)x\|codec_over_rust=([\d.]+)x",
     lambda m: [("serde_string_construct", "flinkable", m[0], "ns/rec", LB),
                ("serde_string_construct", "flink_jvm", m[2], "ns/rec", LB),
                ("serde_string_construct", "flink_over_flinkable", m[4], "x", HB),
                ("serde_string_codec", "flinkable", m[1], "ns/rec", LB),
                ("serde_string_codec", "flink_jvm", m[3], "ns/rec", LB),
                ("serde_string_codec", "flink_over_flinkable", m[5], "x", HB)]),
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
    # M148: backpressure scenario matrix. Every leg reports absolute process RSS
    # (rss_base_mb before the stall, rss_peak_mb while the consumer is stalled,
    # rss_after_mb once it drains) plus what the queue actually absorbed and how
    # many writes the engine refused.
    (r"BP\|flow\|buffers=\d+\|size=\d+\|peak_buffered_mb=([\d.]+)\|produce_ns=([\d.]+)\|rss_base_mb=([\d.]+)\|rss_mb=([\d.]+)",
     lambda m: [("backpressure_tcp_flow", "backlog_mb", m[0], "MB", LB),
                ("backpressure_tcp_flow", "produce_ns", m[1], "ns/buffer", LB),
                ("backpressure_tcp_flow", "rss_base_mb", m[2], "MB", LB),
                ("backpressure_tcp_flow", "rss_peak_mb", m[3], "MB", LB)]),
    (r"BP\|stall\|buffers=\d+\|size=\d+\|accepted=\d+\|refused=(\d+)\|queued_frames=\d+\|queued_mb=[\d.]+\|buffered_mb=([\d.]+)\|peak_mb=[\d.]+\|produce_ns=([\d.]+)\|rss_base_mb=([\d.]+)\|rss_mb=([\d.]+)",
     lambda m: [("backpressure_tcp_stall", "refused", m[0], "buffers", LB),
                ("backpressure_tcp_stall", "backlog_mb", m[1], "MB", LB),
                ("backpressure_tcp_stall", "produce_ns", m[2], "ns/buffer", LB),
                ("backpressure_tcp_stall", "rss_base_mb", m[3], "MB", LB),
                ("backpressure_tcp_stall", "rss_peak_mb", m[4], "MB", LB)]),
    (r"BP\|drain\|frames=\d+\|ns_per_frame=([\d.]+)\|residual_mb=([\d.]+)\|rss_after_mb=([\d.]+)",
     lambda m: [("backpressure_tcp_stall", "drain_ns", m[0], "ns/buffer", LB),
                ("backpressure_tcp_stall", "residual_mb", m[1], "MB", LB),
                ("backpressure_tcp_stall", "rss_after_mb", m[2], "MB", LB)]),
    (r"BP\|broadcast\|channels=\d+\|size=\d+\|accepted=\d+\|refused=(\d+)\|hot_mb=([\d.]+)\|max_healthy_mb=([\d.]+)\|produce_ns=([\d.]+)\|rss_base_mb=([\d.]+)\|rss_mb=([\d.]+)",
     lambda m: [("backpressure_broadcast", "refused", m[0], "buffers", LB),
                ("backpressure_broadcast", "hot_mb", m[1], "MB", LB),
                ("backpressure_broadcast", "max_healthy_mb", m[2], "MB", LB),
                ("backpressure_broadcast", "produce_ns", m[3], "ns/buffer", LB),
                ("backpressure_broadcast", "rss_base_mb", m[4], "MB", LB),
                ("backpressure_broadcast", "rss_peak_mb", m[5], "MB", LB)]),
    (r"BP\|broadcast_recover\|frames=\d+\|ns_per_frame=([\d.]+)\|hot_residual_mb=([\d.]+)\|rss_after_mb=([\d.]+)",
     lambda m: [("backpressure_broadcast", "drain_ns", m[0], "ns/buffer", LB),
                ("backpressure_broadcast", "residual_mb", m[1], "MB", LB),
                ("backpressure_broadcast", "rss_after_mb", m[2], "MB", LB)]),
    (r"BP\|skew\|channels=\d+\|size=\d+\|accepted=\d+\|refused=(\d+)\|hot_mb=([\d.]+)\|max_healthy_mb=([\d.]+)\|produce_ns=([\d.]+)\|rss_base_mb=([\d.]+)\|rss_mb=([\d.]+)",
     lambda m: [("backpressure_skew", "refused", m[0], "buffers", LB),
                ("backpressure_skew", "hot_mb", m[1], "MB", LB),
                ("backpressure_skew", "max_healthy_mb", m[2], "MB", LB),
                ("backpressure_skew", "produce_ns", m[3], "ns/buffer", LB),
                ("backpressure_skew", "rss_base_mb", m[4], "MB", LB),
                ("backpressure_skew", "rss_peak_mb", m[5], "MB", LB)]),
    (r"BP\|skew_recover\|frames=\d+\|ns_per_frame=([\d.]+)\|hot_residual_mb=([\d.]+)\|rss_after_mb=([\d.]+)",
     lambda m: [("backpressure_skew", "drain_ns", m[0], "ns/buffer", LB),
                ("backpressure_skew", "residual_mb", m[1], "MB", LB),
                ("backpressure_skew", "rss_after_mb", m[2], "MB", LB)]),
    (r"BP\|union\|channels=\d+\|size=\d+\|accepted=\d+\|refused=(\d+)\|hot_mb=([\d.]+)\|others_mb=([\d.]+)\|push_ns=([\d.]+)\|rss_base_mb=([\d.]+)\|rss_mb=([\d.]+)",
     lambda m: [("backpressure_union", "refused", m[0], "buffers", LB),
                ("backpressure_union", "hot_mb", m[1], "MB", LB),
                ("backpressure_union", "max_healthy_mb", m[2], "MB", LB),
                ("backpressure_union", "produce_ns", m[3], "ns/record", LB),
                ("backpressure_union", "rss_base_mb", m[4], "MB", LB),
                ("backpressure_union", "rss_peak_mb", m[5], "MB", LB)]),
    (r"BP\|union_recover\|records=\d+\|ns_per_record=([\d.]+)\|residual_mb=([\d.]+)\|rss_after_mb=([\d.]+)",
     lambda m: [("backpressure_union", "drain_ns", m[0], "ns/record", LB),
                ("backpressure_union", "residual_mb", m[1], "MB", LB),
                ("backpressure_union", "rss_after_mb", m[2], "MB", LB)]),
    (r"BP\|capture\|records=\d+\|size=\d+\|queued_mb=([\d.]+)\|captured_mb=([\d.]+)\|capture_ns=([\d.]+)\|refused=(\d+)\|rss_base_mb=([\d.]+)\|rss_queued_mb=([\d.]+)\|rss_peek_mb=([\d.]+)\|rss_mb=([\d.]+)",
     lambda m: [("backpressure_capture", "backlog_mb", m[0], "MB", LB),
                ("backpressure_capture", "captured_mb", m[1], "MB", LB),
                ("backpressure_capture", "capture_ns", m[2], "ns/buffer", LB),
                ("backpressure_capture", "refused", m[3], "buffers", LB),
                ("backpressure_capture", "rss_base_mb", m[4], "MB", LB),
                ("backpressure_capture", "rss_queued_mb", m[5], "MB", LB),
                ("backpressure_capture", "rss_peek_mb", m[6], "MB", LB),
                ("backpressure_capture", "rss_peak_mb", m[7], "MB", LB)]),
    # M149: the same shapes against the producer-side budget (pool cap + refusal).
    (r"BP\|capped\|buffers=\d+\|size=\d+\|accepted=\d+\|refused=(\d+)\|buffered_mb=([\d.]+)\|produce_ns=([\d.]+)\|rss_base_mb=([\d.]+)\|rss_mb=([\d.]+)",
     lambda m: [("backpressure_tcp_stall_capped", "refused", m[0], "buffers", LB),
                ("backpressure_tcp_stall_capped", "backlog_mb", m[1], "MB", LB),
                ("backpressure_tcp_stall_capped", "produce_ns", m[2], "ns/buffer", LB),
                ("backpressure_tcp_stall_capped", "rss_base_mb", m[3], "MB", LB),
                ("backpressure_tcp_stall_capped", "rss_peak_mb", m[4], "MB", LB)]),
    (r"BP\|capped_drain\|frames=\d+\|ns_per_frame=([\d.]+)\|residual_mb=([\d.]+)\|held_after=\d+\|rss_after_mb=([\d.]+)",
     lambda m: [("backpressure_tcp_stall_capped", "drain_ns", m[0], "ns/buffer", LB),
                ("backpressure_tcp_stall_capped", "residual_mb", m[1], "MB", LB),
                ("backpressure_tcp_stall_capped", "rss_after_mb", m[2], "MB", LB)]),
    (r"BP\|exchange_capped\|records=\d+\|size=\d+\|accepted=\d+\|refused=(\d+)\|queued_mb=([\d.]+)\|push_ns=([\d.]+)\|rss_base_mb=([\d.]+)\|rss_mb=([\d.]+)",
     lambda m: [("backpressure_channel_capped", "refused", m[0], "records", LB),
                ("backpressure_channel_capped", "backlog_mb", m[1], "MB", LB),
                ("backpressure_channel_capped", "produce_ns", m[2], "ns/record", LB),
                ("backpressure_channel_capped", "rss_base_mb", m[3], "MB", LB),
                ("backpressure_channel_capped", "rss_peak_mb", m[4], "MB", LB)]),
    (r"BP\|exchange_capped_drain\|records=\d+\|ns_per_record=([\d.]+)\|residual_mb=([\d.]+)\|rss_after_mb=([\d.]+)",
     lambda m: [("backpressure_channel_capped", "drain_ns", m[0], "ns/record", LB),
                ("backpressure_channel_capped", "residual_mb", m[1], "MB", LB),
                ("backpressure_channel_capped", "rss_after_mb", m[2], "MB", LB)]),
    (r"BP\|union_capped\|channels=\d+\|size=\d+\|accepted=\d+\|refused=(\d+)\|hot_mb=([\d.]+)\|others_mb=([\d.]+)\|push_ns=([\d.]+)\|rss_base_mb=([\d.]+)\|rss_mb=([\d.]+)",
     lambda m: [("backpressure_union_capped", "refused", m[0], "records", LB),
                ("backpressure_union_capped", "hot_mb", m[1], "MB", LB),
                ("backpressure_union_capped", "max_healthy_mb", m[2], "MB", LB),
                ("backpressure_union_capped", "produce_ns", m[3], "ns/record", LB),
                ("backpressure_union_capped", "rss_base_mb", m[4], "MB", LB),
                ("backpressure_union_capped", "rss_peak_mb", m[5], "MB", LB)]),
    (r"BP\|union_capped_recover\|records=\d+\|ns_per_record=([\d.]+)\|residual_mb=([\d.]+)\|rss_after_mb=([\d.]+)",
     lambda m: [("backpressure_union_capped", "drain_ns", m[0], "ns/record", LB),
                ("backpressure_union_capped", "residual_mb", m[1], "MB", LB),
                ("backpressure_union_capped", "rss_after_mb", m[2], "MB", LB)]),
    (r"BP\|capture_release\|residual_mb=([\d.]+)\|rss_after_mb=([\d.]+)",
     lambda m: [("backpressure_capture", "residual_mb", m[0], "MB", LB),
                ("backpressure_capture", "rss_after_mb", m[1], "MB", LB)]),
    # M150: a parked producer — wake latency after a drain frees budget, and the
    # flat RSS while it stalls (the record stays in the producer, not the queue).
    (r"BP\|park\|rounds=\d+\|size=\d+\|wake_ns=([\d.]+)\|parked_ns=([\d.]+)\|rss_parked_mb=([\d.]+)",
     lambda m: [("backpressure_park", "wake_ns", m[0], "ns", LB),
                ("backpressure_park", "parked_ns", m[1], "ns", LB),
                ("backpressure_park", "rss_parked_mb", m[2], "MB", LB)]),
    (r"BP\|exchange\|records=\d+\|size=\d+\|accepted=\d+\|refused=(\d+)\|queued_mb=([\d.]+)\|push_ns=([\d.]+)\|rss_base_mb=([\d.]+)\|rss_mb=([\d.]+)",
     lambda m: [("backpressure_channel", "refused", m[0], "records", LB),
                ("backpressure_channel", "backlog_mb", m[1], "MB", LB),
                ("backpressure_channel", "produce_ns", m[2], "ns/record", LB),
                ("backpressure_channel", "rss_base_mb", m[3], "MB", LB),
                ("backpressure_channel", "rss_peak_mb", m[4], "MB", LB)]),
    (r"BP\|exchange_drain\|records=\d+\|ns_per_record=([\d.]+)\|residual_mb=([\d.]+)\|rss_after_mb=([\d.]+)",
     lambda m: [("backpressure_channel", "drain_ns", m[0], "ns/record", LB),
                ("backpressure_channel", "residual_mb", m[1], "MB", LB),
                ("backpressure_channel", "rss_after_mb", m[2], "MB", LB)]),
    # M148 S7: the stalled consumer is an embedded Python operator. The RSS
    # steps decompose the bar into engine / interpreter / libraries / queue.
    (r"RSS\|embed\|base_mb=([\d.]+)\|interp_mb=([\d.]+)\|pandas_mb=([\d.]+)\|batch_mb=([\d.]+)",
     lambda m: [("memory_embedded_python", "engine_base_mb", m[0], "MB", LB),
                ("memory_embedded_python", "interp_total_mb", m[1], "MB", LB),
                ("memory_embedded_python", "libs_total_mb", m[2], "MB", LB),
                ("memory_embedded_python", "batch_total_mb", m[3], "MB", LB)]),
    (r"BP\|python\|batches=\d+\|rows_per_batch=\d+\|queued_mb=([\d.]+)\|rss_base_mb=([\d.]+)\|rss_stall_mb=([\d.]+)\|rss_after_mb=([\d.]+)\|drain_ms=(\d+)",
     lambda m: [("backpressure_python", "backlog_mb", m[0], "MB", LB),
                ("backpressure_python", "rss_base_mb", m[1], "MB", LB),
                ("backpressure_python", "rss_peak_mb", m[2], "MB", LB),
                ("backpressure_python", "rss_after_mb", m[3], "MB", LB),
                ("backpressure_python", "drain_ms", m[4], "ms", LB)]),
    # M148 JVM reference legs: the same shapes against Flink's own buffer pools.
    (r"BPJ\|pool\|segments=\d+\|size=\d+\|granted=\d+\|refused=(\d+)\|parked_mb=([\d.]+)\|available=\w+\|reservation_mb=([\d.]+)\|produce_ns=(\d+)\|rss_base_mb=([\d.]+)\|rss_mb=([\d.]+)",
     lambda m: [("backpressure_tcp_stall_jvm", "refused", m[0], "buffers", LB),
                ("backpressure_tcp_stall_jvm", "backlog_mb", m[1], "MB", LB),
                ("backpressure_tcp_stall_jvm", "reservation_mb", m[2], "MB", LB),
                ("backpressure_tcp_stall_jvm", "produce_ns", m[3], "ns/buffer", LB),
                ("backpressure_tcp_stall_jvm", "rss_base_mb", m[4], "MB", LB),
                ("backpressure_tcp_stall_jvm", "rss_peak_mb", m[5], "MB", LB)]),
    (r"BPJ\|pool_recover\|ns_per_buffer=(\d+)\|available=\w+\|rss_after_mb=([\d.]+)",
     lambda m: [("backpressure_tcp_stall_jvm", "drain_ns", m[0], "ns/buffer", LB),
                ("backpressure_tcp_stall_jvm", "rss_after_mb", m[1], "MB", LB)]),
    (r"BPJ\|reserved\|segments=\d+\|size=\d+\|granted=\d+\|refused=(\d+)\|reserved_mb=([\d.]+)\|produce_ns=(\d+)\|rss_base_mb=([\d.]+)\|rss_mb=([\d.]+)",
     lambda m: [("backpressure_reserved_jvm", "refused", m[0], "buffers", LB),
                ("backpressure_reserved_jvm", "backlog_mb", m[1], "MB", LB),
                ("backpressure_reserved_jvm", "produce_ns", m[2], "ns/buffer", LB),
                ("backpressure_reserved_jvm", "rss_base_mb", m[3], "MB", LB),
                ("backpressure_reserved_jvm", "rss_peak_mb", m[4], "MB", LB)]),
    (r"BPJ\|reserved_recover\|ns_per_buffer=(\d+)\|rss_after_mb=([\d.]+)",
     lambda m: [("backpressure_reserved_jvm", "drain_ns", m[0], "ns/buffer", LB),
                ("backpressure_reserved_jvm", "rss_after_mb", m[1], "MB", LB)]),
    (r"BPJ\|broadcast\|channels=\d+\|size=\d+\|accepted=\d+\|refused=(\d+)\|hot_mb=([\d.]+)\|max_healthy_mb=([\d.]+)\|produce_ns=(\d+)\|rss_base_mb=([\d.]+)\|rss_mb=([\d.]+)",
     lambda m: [("backpressure_broadcast_jvm", "refused", m[0], "buffers", LB),
                ("backpressure_broadcast_jvm", "hot_mb", m[1], "MB", LB),
                ("backpressure_broadcast_jvm", "max_healthy_mb", m[2], "MB", LB),
                ("backpressure_broadcast_jvm", "produce_ns", m[3], "ns/buffer", LB),
                ("backpressure_broadcast_jvm", "rss_base_mb", m[4], "MB", LB),
                ("backpressure_broadcast_jvm", "rss_peak_mb", m[5], "MB", LB)]),
    (r"BPJ\|broadcast_recover\|rss_after_mb=([\d.]+)",
     lambda m: [("backpressure_broadcast_jvm", "rss_after_mb", m[0], "MB", LB)]),
    (r"BPJ\|skew\|channels=\d+\|size=\d+\|accepted=\d+\|refused=(\d+)\|hot_mb=([\d.]+)\|max_healthy_mb=([\d.]+)\|produce_ns=(\d+)\|rss_base_mb=([\d.]+)\|rss_mb=([\d.]+)",
     lambda m: [("backpressure_skew_jvm", "refused", m[0], "buffers", LB),
                ("backpressure_skew_jvm", "hot_mb", m[1], "MB", LB),
                ("backpressure_skew_jvm", "max_healthy_mb", m[2], "MB", LB),
                ("backpressure_skew_jvm", "produce_ns", m[3], "ns/buffer", LB),
                ("backpressure_skew_jvm", "rss_base_mb", m[4], "MB", LB),
                ("backpressure_skew_jvm", "rss_peak_mb", m[5], "MB", LB)]),
    (r"BPJ\|skew_recover\|rss_after_mb=([\d.]+)",
     lambda m: [("backpressure_skew_jvm", "rss_after_mb", m[0], "MB", LB)]),
    (r"BPJ\|capture\|records=\d+\|size=\d+\|queued_mb=([\d.]+)\|captured_mb=([\d.]+)\|capture_ns=(\d+)\|rss_base_mb=([\d.]+)\|rss_queued_mb=([\d.]+)\|rss_mb=([\d.]+)",
     lambda m: [("backpressure_capture_jvm", "backlog_mb", m[0], "MB", LB),
                ("backpressure_capture_jvm", "captured_mb", m[1], "MB", LB),
                ("backpressure_capture_jvm", "capture_ns", m[2], "ns/buffer", LB),
                ("backpressure_capture_jvm", "rss_base_mb", m[3], "MB", LB),
                ("backpressure_capture_jvm", "rss_queued_mb", m[4], "MB", LB),
                ("backpressure_capture_jvm", "rss_peak_mb", m[5], "MB", LB)]),
    (r"BPJ\|capture_release\|residual_mb=([\d.]+)\|rss_after_mb=([\d.]+)",
     lambda m: [("backpressure_capture_jvm", "residual_mb", m[0], "MB", LB),
                ("backpressure_capture_jvm", "rss_after_mb", m[1], "MB", LB)]),
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
