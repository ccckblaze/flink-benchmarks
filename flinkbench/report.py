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
"""Markdown report generation from the local store — tables, regression, chart.

Layout mirrors what a flink-benchmarks Codespeed page tells you per benchmark
(latest value, unit, direction) but consolidated into one document with the
same-screen chart embedded. Terminology always goes through `terms.py`.
"""

from . import chart, regression
from .terms import case_label, metric_label, unit_label

ORDER = [
    "serde_int_long", "serde_string",
    "smallbatch_16", "smallbatch_128", "smallbatch_1024", "smallbatch_8192",
    "compute_arrow_native", "compute_pandas",
    "memory_engine_rss",
    "framing_encode", "framing_decode", "framing_wire", "shuffle_take",
    "embed_interface", "operator_fusion", "network_accounting", "cross_task_hop_floor",
    "serde_owned_form_int_long", "serde_owned_form_string",
]


def _latest_rows(con):
    rows = con.execute(
        """SELECT m.case_name, m.metric, m.value, m.unit, m.direction FROM metrics m
           JOIN runs r ON r.id = m.run_id
           JOIN (SELECT case_name, metric, max(r2.ts || ':' || printf('%09d', r2.id)) AS k
                 FROM metrics m2 JOIN runs r2 ON r2.id = m2.run_id
                 GROUP BY case_name, metric) t
             ON t.case_name = m.case_name AND t.metric = m.metric
            AND t.k = r.ts || ':' || printf('%09d', r.id)
           ORDER BY m.case_name, m.metric"""
    ).fetchall()
    return rows


def _has_case(con, case):
    return con.execute("SELECT 1 FROM metrics WHERE case_name = ? LIMIT 1", (case,)).fetchone() is not None


def _value(con, case, metric):
    row = chart._latest(con, case, metric)
    return row[0] if row else 0.0


def generate(con, out_dir, title, env_lines):
    import os

    os.makedirs(out_dir, exist_ok=True)
    png = "report.png"
    chart.render(con, os.path.join(out_dir, png), title)

    seen = set()
    rows = _latest_rows(con)
    lines = ["# " + title, ""]
    lines += ["%s" % env for env in env_lines]
    lines += ["", "![关键指标同屏对比](%s)" % png, ""]
    lines += ["## 指标明细（每项取最近一次摄取值）", ""]
    lines += ["| 指标 | 度量 | 数值 | 方向 |", "| --- | --- | --- | --- |"]
    ordered = [c for c in ORDER if any(r[0] == c for r in rows)]
    ordered += sorted({r[0] for r in rows} - set(ordered))
    for case in ordered:
        for c, metric, value, unit, direction in rows:
            if c != case:
                continue
            seen.add((case, metric))
            arrow = "越低越好" if direction == "lower_is_better" else "越高越好"
            lines.append("| %s | %s | %.4g %s | %s |" % (
                case_label(case), metric_label(metric), value, unit_label(unit), arrow))
    lines += ["", "## 非 ASCII 字符串全链条（与原版同会话、同形状、同纪律并列）", ""]
    canonical = [("serde_shape_cjk10", "中文 10 字"),
                 ("serde_shape_cjk40", "中文 40 字"),
                 ("serde_shape_mixed", "中英混排 + emoji")]
    canon_rows = [(label, _value(con, case, "flinkable"), _value(con, case, "flink_jvm"),
                   _value(con, case, "flink_over_flinkable"))
                  for case, label in canonical if _has_case(con, case)]
    if canon_rows:
        lines += ["| 形状 | flinkable（Rust 原生） | Apache Flink 原版（JVM） | 原版/flinkable |",
                  "| --- | --- | --- | --- |"]
        for label, native, jvm, ratio in canon_rows:
            lines.append("| %s | %.0f 纳秒/记录 | %.0f 纳秒/记录 | %.2f 倍 |" % (
                label, native, jvm, ratio))
        lines += ["", "口径：整条链（值构造 + 编码 + 解码）每记录往返耗时，纳秒/记录；两腿取自同一次"
                      "跨引擎对表运行，Rust 腿为池化批形制（生产形态），JVM 腿为 Flink"
                      " `StringSerializer` 经 `DataOutputSerializer`/`DataInputDeserializer` 的完整往返。"]
    lines += ["", "## 编解码通道改造前后（同一二进制内的 A/B 腿）", ""]
    shape_specs = [("ab_encode_cjk_8", "ab_decode_cjk_8", "中文 10 字"),
                   ("ab_encode_cjk_64", "ab_decode_cjk_64", "中文 52 字"),
                   ("ab_encode_kana_16", "ab_decode_kana_16", "日文 16 字"),
                   ("ab_encode_emoji_5", "ab_decode_emoji_5", "emoji"),
                   ("ab_encode_mixed_16", "ab_decode_mixed_16", "中英混排 + emoji"),
                   ("ab_encode_ascii_12", "ab_decode_ascii_12", "ASCII 12 字节（对照）"),
                   ("ab_encode_ascii_47", "ab_decode_ascii_47", "ASCII 47 字节（对照）")]
    shape_rows = []
    for enc, dec, label in shape_specs:
        if not (_has_case(con, enc) and _has_case(con, dec)):
            continue
        before = _value(con, enc, "flinkable_before") + _value(con, dec, "flinkable_before")
        after = _value(con, enc, "flinkable") + _value(con, dec, "flinkable")
        borrow = _value(con, dec, "flinkable_borrow")
        if before and after:
            shape_rows.append((label, before, after, borrow))
    if shape_rows:
        lines += ["| 形状 | 改造前 | 改造后 | 提速 | 借用零分配解码 |",
                  "| --- | --- | --- | --- | --- |"]
        for label, before, after, borrow in shape_rows:
            lines.append("| %s | %.0f | %.0f | %.0f%% | %.0f |" % (
                label, before, after, (before - after) * 100.0 / before, borrow))
        lines += ["", "口径：仅编解码通道（纳秒/记录），不含值构造项；“改造前/改造后”为同一次运行内"
                      "并列的 A/B 腿，避免跨会话机器状态漂移；“借用零分配解码”为解码不新建字符串的"
                      "下界形态，仅作参照。"]
    lines += ["", "## 时间线趋势（同一指标随摄取次序，Codespeed 式核心视图）", ""]
    trend_runs = con.execute(
        "SELECT id, source FROM runs WHERE suite != 'manual' ORDER BY id").fetchall()
    trend_cases = []
    for case in ("serde_string", "serde_int_long"):
        if con.execute("SELECT count(DISTINCT run_id) FROM metrics WHERE case_name = ?",
                       (case,)).fetchone()[0] >= 2:
            trend_cases.append(case)
    if trend_cases:
        header = "| 指标 | 度量 | " + " | ".join(src for (_i, src) in trend_runs) + " |"
        lines += [header, "| --- | --- | " + " | ".join("---" for _ in trend_runs) + " |"]
        for case in trend_cases:
            for metric in ("flinkable", "flink_jvm"):
                cells = []
                for rid, _src in trend_runs:
                    row = con.execute(
                        "SELECT value FROM metrics WHERE run_id = ? AND case_name = ? AND metric = ?",
                        (rid, case, metric)).fetchone()
                    cells.append("%.0f" % row[0] if row else "—")
                if any(c != "—" for c in cells):
                    lines.append("| %s | %s | %s |" % (
                        case_label(case), metric_label(metric), " | ".join(cells)))
    lines += ["", "## 回归检查（基线窗口算法，移植自上游 regression_report_v2）", ""]
    lines += ["| 指标 | 度量 | 状态 |", "| --- | --- | --- |"]
    insufficient = 0
    judged = 0
    for case, metric, status, detail in regression.scan_store(con):
        if status == "insufficient_samples":
            insufficient += 1
            continue
        judged += 1
        note = ("回退 %.1f%%（阈值 %.1f%%）" % (
            detail["regression_ratio"] * 100, detail["threshold"] * 100)
            if status == "regression" else "正常")
        lines.append("| %s | %s | %s |" % (case_label(case), metric_label(metric), note))
    if judged == 0:
        lines.append("| （已判定项） | — | 无回退 |")
    if insufficient:
        lines += ["", "另有 %d 项序列摄取次数不足 5 次，按上游方法论不参与回归判定，"
                      "仅计入趋势面板。" % insufficient]
    lines += ["", "## 术语对照", "",
              "| 报告用词 | Flink 文档对应 |", "| --- | --- |",
              "| 序列化 / 反序列化 | Serialization / Deserialization |",
              "| 每记录耗时 | per-record latency |",
              "| 批大小 | batch size |",
              "| 零拷贝 | zero-copy（Apache Arrow） |",
              "| 进程间通道 | inter-process data plane（Beam worker，Arrow IPC） |",
              "| 常驻内存（RSS） | Resident Set Size |",
              "| 改造前 / 改造后 | before / after（同一二进制内的 A/B 腿） |",
              "| 借用零分配 | zero-copy decode，借用输入缓冲不新建字符串 |",
              "| 值构造项 | 基准里构造输入对象的费用，非引擎开销 |",
              "| 同会话 | 同一台机器、同一次运行内并列测量（跨会话不可比） |",
              "| 非 ASCII 形状 | 中文 / 日文 / emoji（UTF-16 码元多字节形制） |",
              "| 吞吐量 | throughput |",
              "| shuffle 随机取数 | shuffle read（Arrow take） |", ""]
    path = os.path.join(out_dir, "report.md")
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")
    return path
