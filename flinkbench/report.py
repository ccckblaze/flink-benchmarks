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
    lines += ["", "## 非 ASCII 字符串全链条（与 Flink 同会话、同形状、同纪律并列）", ""]
    canonical = [("serde_shape_cjk10", "中文 10 字"),
                 ("serde_shape_cjk40", "中文 40 字"),
                 ("serde_shape_mixed", "中英混排 + emoji")]
    canon_rows = [(label, _value(con, case, "flinkable"), _value(con, case, "flink_jvm"),
                   _value(con, case, "flink_over_flinkable"))
                  for case, label in canonical if _has_case(con, case)]
    if canon_rows:
        lines += ["| 形状 | Flinkable | Flink | Flink/Flinkable |",
                  "| --- | --- | --- | --- |"]
        for label, native, jvm, ratio in canon_rows:
            lines.append("| %s | %.0f 纳秒/记录 | %.0f 纳秒/记录 | %.2f 倍 |" % (
                label, native, jvm, ratio))
        lines += ["", "口径：整条链（值构造 + 编码 + 解码）每记录往返耗时，纳秒/记录；两腿取自同一次"
                      "跨引擎对表运行，Flinkable 腿为池化批形制（生产形态），Flink 腿为"
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
    lines += ["", "## 反压分型：停读期间与排空后的绝对内存", ""]

    def _maybe(con, case, metric):
        row = chart._latest(con, case, metric)
        return row[0] if row else None

    bp_matrix = [
        # M153: the Flinkable budget is default-on (`Exchange::new` = 16×32 KB,
        # `ShuffleServer::start` = 16/64×32 KB), so the default-config rows are
        # the capped legs; the unbounded rows are marked 显式关闭预算 — they are
        # the opt-out. Flink's reserved-pool proxy row stays, but never as a
        # cross-engine pairing (chart ⑦ shows 设预算 pairs only).
        ("网络通道停读", "Flinkable·设预算", "backpressure_tcp_stall_capped"),
        ("网络通道停读", "Flink·设预算", "backpressure_tcp_stall_jvm"),
        ("网络通道停读", "Flinkable·显式关闭预算", "backpressure_tcp_stall"),
        ("网络通道停读", "Flink·预留全场景（预算强制的代理）", "backpressure_reserved_jvm"),
        ("广播扇出·一慢", "Flinkable·设预算", "backpressure_broadcast_capped"),
        ("广播扇出·一慢", "Flink·设预算", "backpressure_broadcast_jvm"),
        ("广播扇出·一慢", "Flinkable·显式关闭预算", "backpressure_broadcast"),
        ("数据倾斜·一热", "Flinkable·设预算", "backpressure_skew_capped"),
        ("数据倾斜·一热", "Flink·设预算", "backpressure_skew_jvm"),
        ("数据倾斜·一热", "Flinkable·显式关闭预算", "backpressure_skew"),
        ("多输入归并·一慢（Flinkable 内部边）", "Flinkable·设预算", "backpressure_union_capped"),
        ("多输入归并·一慢（Flinkable 内部边）", "Flinkable·显式关闭预算", "backpressure_union"),
        ("任务间通道停读（Flinkable 内部边）", "Flinkable·设预算", "backpressure_channel_capped"),
        ("任务间通道停读（Flinkable 内部边）", "Flinkable·显式关闭预算", "backpressure_channel"),
        ("非对齐检查点在途快照", "Flinkable", "backpressure_capture"),
        ("非对齐检查点在途快照", "Flink", "backpressure_capture_jvm"),
        ("Python 算子停读（嵌入式）", "Flinkable", "backpressure_python"),
    ]
    bp_rows = []
    for shape, engine, case in bp_matrix:
        if not _has_case(con, case):
            continue
        base = _maybe(con, case, "rss_base_mb")
        peak = _maybe(con, case, "rss_peak_mb")
        after = _maybe(con, case, "rss_after_mb")
        queued = _maybe(con, case, "rss_queued_mb")
        peek = _maybe(con, case, "rss_peek_mb")
        held = _maybe(con, case, "backlog_mb")
        if held is None:
            held = _maybe(con, case, "hot_mb")
        refused = _maybe(con, case, "refused")
        copy_ref = peek if peek is not None else queued
        copy_mb = (peak - copy_ref) if (peak is not None and copy_ref is not None) else None
        bp_rows.append((shape, engine, base, peak, after, held, copy_mb, refused))
    if bp_rows:
        lines += ["| 场景 | 引擎 | 基线 RSS | 反压峰值 | 排空后 | 队列/池驻留 | 快照额外拷贝 | 拒绝写入 |",
                  "| --- | --- | --- | --- | --- | --- | --- | --- |"]
        for shape, engine, base, peak, after, held, copy_mb, refused in bp_rows:
            cell = lambda v, fmt="%.1f": ("—" if v is None else fmt % v)
            lines.append("| %s | %s | %s | %s | %s | %s | %s | %s |" % (
                shape, engine, cell(base), cell(peak), cell(after), cell(held),
                cell(copy_mb), "—" if refused is None else "%d 次" % int(refused)))
        interp_note = ""
        if _has_case(con, "memory_embedded_python"):
            interp_note = ("嵌入式 Python 的构成（同会话探针，`RSS|embed`）：Flinkable 内核基线 %.1f MB → "
                           "CPython 解释器 %.1f MB → pyarrow/pandas 库 %.1f MB → 在途 Arrow 批 %.1f MB；"
                           "Python 算子停读行的色块即按此拆分。" % (
                               _maybe(con, "memory_embedded_python", "engine_base_mb"),
                               _maybe(con, "memory_embedded_python", "interp_total_mb")
                               - _maybe(con, "memory_embedded_python", "engine_base_mb"),
                               _maybe(con, "memory_embedded_python", "libs_total_mb")
                               - _maybe(con, "memory_embedded_python", "interp_total_mb"),
                               _maybe(con, "memory_embedded_python", "batch_total_mb")
                               - _maybe(con, "memory_embedded_python", "libs_total_mb")))
        lines += ["", "口径：全部为**同一进程的绝对常驻内存 RSS**（`/proc/self/statm`），三段采样——停读前基线、"
                      "停读期间峰值、恢复排空后终值；不再报告增量。Flink 对照腿在**独立 JVM 进程**逐场景运行，"
                      "基线互不污染。要点：Flink“反压时内存涨得快”发生在**构造期预留**（缓冲池一次性握段），"
                      "“回收慢甚至不回收”是**池化复用**（段不回 OS，全程 GC 仅 2–3 次），均非 GC 问题；"
                      "每通道配额（`maxBuffersPerChannel`）使一慢/一热被封顶在 2 MB 级，拒绝 1984 次。"
                      "M149 已为 Flinkable 补上同形信号：任务间通道（Exchange）与网络 shuffle 均有生产者"
                      "侧预算封顶（`with_budget`/`try_produce`），三形状拒绝 8128/8128/1984 次与 Flink 对照腿"
                      "一致，RSS 仅 +2 MB 级且排空后全部回落基线——无界积压与残留两个缺口关闭。"
                      "M150 进一步：①非对齐快照的 in-flight 数据改为 `bytes::Bytes` 引用计数共享，"
                      "持久化拷贝从 64 MB 归零——capture 峰值由 2×（131.3）回落到 1×+0.1（67.3），"
                      "与 Flink BufferConsumer 共享同一段内存完全同形；②预算满时生产者不再只是被拒，"
                      "而是在 emit 内**阻塞停读**（`push_blocking`，忠实 `requestBufferBuilderBlocking`），"
                      "腾出预算后 44µs 唤醒、停读期间 RSS 持平基线——一个满队列就此逐级回传为端到端反压。"
                      "未封顶形状排空后 RSS 是否回落取决于分配器能否归还"
                      "连续空闲块（归并/广播回落，停读/倾斜部分保留），属碎片而非泄漏。"
                      "**控制变量（M153）**：Flink 的网络缓冲预算是强制的，没有真正的“未设预算”——"
                      "Flinkable 已把预算改为**默认开启**（`Exchange::new` 16×32 KB、`ShuffleServer::start` "
                      "每通道 16 段/全局 64 段，与 Flink `maxBuffersPerChannel=16`、`NetworkBufferPool` 同配额），"
                      "默认配置即与 Flink 同为“设预算”，图⑦只保留可配平的设预算柱对；"
                      "表中 显式关闭预算 行是 `unbounded()` 选离的对照腿，仅用于展示预算的边际效果。"
                      "广播/倾斜封顶后热通道 0.5 MB、RSS 5.3/4.7 MB，与 Flink·设预算 的 83/84 MB 同口径可比。"
                      "任务间通道/多输入归并为 Flinkable 进程内边，Flink 无对应，单列不参与跨引擎配平。" + interp_note]

    # M151: end-to-end backpressure — the producer-side park, priced for both
    # the in-process edge and the network data plane. These are latency/flatness
    # numbers, not an RSS decomposition, so they get their own table.
    park_rows = []
    for engine, case in (("进程内边（Exchange/链尾）", "backpressure_park"),
                         ("网络数据面（shuffle TCP）", "backpressure_tcp_park")):
        if not _has_case(con, case):
            continue
        park_rows.append((engine, _maybe(con, case, "wake_ns"),
                          _maybe(con, case, "rss_parked_mb")))
    if park_rows:
        lines += ["", "## 端到端反压：生产者停读唤醒", "",
                  "| 边类型 | 预算腾出后唤醒（µs） | 停读期间 RSS（MB） |",
                  "| --- | --- | --- |"]
        for engine, wake, rss in park_rows:
            cell = lambda v, fmt="%.0f": ("—" if v is None else fmt % v)
            lines.append("| %s | %s | %s |" % (
                engine, cell(None if wake is None else wake / 1000.0), cell(rss, "%.1f")))
        lines += ["", "口径：预算满时生产者不再自旋重试，而是在 emit 内挂起（进程内边 `push_blocking`"
                      " 用 `Condvar`；网络边 `produce_ready` 用 tokio `Notify`，异步侧不占线程），"
                      "忠实 `LocalBufferPool.requestBufferBuilderBlocking`。唤醒延迟 = 消费者排空一个缓冲、"
                      "预算腾出、到生产者恢复推入的往返；网络边含一次 socket flush 故高于进程内边。"
                      "停读期间 RSS 持平基线，正说明满队列被封顶在预算内、逐级回传为端到端反压，"
                      "而非在引擎里无界堆积。"]
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
