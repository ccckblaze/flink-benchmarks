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
"""Same-screen comparison chart, rendered from the local store.

Panels appear only when their family has data; every number comes from the
latest run of its case, so re-ingesting a log and re-rendering is the whole
update cycle. Ratio labels are read from stored ratio metrics — never
recomputed off rounded bar values.
"""

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec

plt.rcParams["font.sans-serif"] = ["PingFang SC", "Hiragino Sans GB", "Arial Unicode MS"]
plt.rcParams["axes.unicode_minus"] = False

BLUE = "#3066be"
GRAY = "#9aa0a6"


def _latest(con, case, metric):
    row = con.execute(
        """SELECT m.value, m.unit FROM metrics m JOIN runs r ON r.id = m.run_id
           WHERE m.case_name = ? AND m.metric = ? ORDER BY r.ts DESC, r.id DESC LIMIT 1""",
        (case, metric),
    ).fetchone()
    return row


def _has(con, case):
    return con.execute("SELECT 1 FROM metrics WHERE case_name = ? LIMIT 1", (case,)).fetchone() is not None


def _bar_labels(ax, bars, fmt="{:.0f}"):
    for bar in bars:
        ax.annotate(fmt.format(bar.get_height()),
                    (bar.get_x() + bar.get_width() / 2, bar.get_height()),
                    ha="center", va="bottom", fontsize=9)


def render(con, out_path, title):
    panels = []
    if _has(con, "serde_int_long") and _has(con, "serde_string"):
        panels.append("serde")
    if all(_has(con, "smallbatch_" + b) for b in ("16", "128", "1024", "8192")):
        panels.append("smallbatch")
    if _has(con, "compute_arrow_native") and _has(con, "compute_pandas"):
        panels.append("compute")
    if _has(con, "memory_engine_rss"):
        panels.append("memory")
    if _has(con, "framing_encode") and _has(con, "framing_decode"):
        panels.append("throughput")

    n = len(panels)
    if n == 0:
        raise SystemExit("no panel data in the store — ingest a run first")
    trend = _trend_series(con)
    panel_rows = (n + 1) // 2
    rows = panel_rows + (1 if trend else 0)
    fig = plt.figure(figsize=(13.2, 4.8 * panel_rows + (3.6 if trend else 0)))
    gs = GridSpec(rows, 2, figure=fig, hspace=0.62, wspace=0.26)
    flat = [fig.add_subplot(gs[i // 2, i % 2]) for i in range(n)]
    fig.suptitle(title, fontsize=15, fontweight="bold", y=0.99)
    w = 0.34

    for idx, (ax, panel) in enumerate(zip(flat, panels)):
        if panel == "serde":
            groups = ["定长字段\n（整数+长整数）", "字符串\n（含构造）"]
            cases = ["serde_int_long", "serde_string"]
            native = [_latest(con, c, "flinkable")[0] for c in cases]
            jvm = [_latest(con, c, "flink_jvm")[0] for c in cases]
            x = range(len(groups))
            b1 = ax.bar([i - w / 2 for i in x], native, w, color=BLUE, label="flinkable（Rust 原生）")
            b2 = ax.bar([i + w / 2 for i in x], jvm, w, color=GRAY, label="Apache Flink 原版（JVM）")
            _bar_labels(ax, list(b1) + list(b2))
            ratio = _latest(con, "serde_int_long", "flink_over_flinkable")
            if ratio:
                ax.annotate("Rust 快 %.2f 倍" % ratio[0], (0, max(native + jvm) * 1.15),
                            ha="center", fontsize=9, color=BLUE)
            ax.set_xticks(list(x), groups)
            ax.set_ylabel("每记录往返耗时（纳秒，越低越好）")
            ax.set_title("① 序列化 / 反序列化往返（字节格式与 Flink 原版完全一致，逐字节校验）", fontsize=10)
            ax.legend(fontsize=8, loc="upper right")
            ax.set_ylim(0, max(native + jvm) * 1.35)
        elif panel == "smallbatch":
            batches = ["16", "128", "1024", "8192"]
            l2 = [_latest(con, "smallbatch_" + b, "l2_zero_copy")[0] for b in batches]
            l1 = [_latest(con, "smallbatch_" + b, "l1_ipc")[0] for b in batches]
            ratios = [_latest(con, "smallbatch_" + b, "transfer_ratio")[0] for b in batches]
            x = range(len(batches))
            ax.bar([i - w / 2 for i in x], l2, w, color=BLUE,
                   label="flinkable 引擎内零拷贝（Apache Arrow）")
            ax.bar([i + w / 2 for i in x], l1, w, color=GRAY,
                   label="原版 PyFlink 进程间通道（Apache Arrow IPC）")
            ax.set_yscale("log")
            for i in x:
                ax.annotate("%.1f×" % ratios[i], (i, max(l1[i], l2[i]) * 1.5), ha="center",
                            fontsize=9, color=BLUE, fontweight="bold")
            ax.set_xticks(list(x), ["%s 行" % b for b in batches])
            ax.set_xlabel("批大小")
            ax.set_ylabel("每记录传输耗时（纳秒，对数轴，越低越好）")
            ax.set_title("② 数据在 Python 算子与引擎之间的往返传输", fontsize=10)
            ax.legend(fontsize=7.5, loc="upper right")
            ax.set_ylim(0.2, max(l1) * 8)
        elif panel == "compute":
            groups = ["向量化计算\n（Apache Arrow）", "pandas 计算"]
            cases = ["compute_arrow_native", "compute_pandas"]
            l2 = [_latest(con, c, "l2_end_to_end")[0] for c in cases]
            l1 = [_latest(con, c, "l1_end_to_end")[0] for c in cases]
            ratios = [_latest(con, c, "end_to_end_ratio")[0] for c in cases]
            x = range(len(groups))
            ax.bar([i - w / 2 for i in x], l2, w, color=BLUE, label="flinkable（传输零拷贝 + 计算）")
            ax.bar([i + w / 2 for i in x], l1, w, color=GRAY, label="原版 PyFlink（进程间通道 + 计算）")
            ax.set_yscale("log")
            _bar_labels(ax, [bar for container in ax.containers for bar in container], "{:.1f}")
            ax.annotate("端到端 %.2f 倍" % ratios[0], (0, l1[0] * 2.2), ha="center", fontsize=9, color=BLUE)
            ax.annotate("%.2f 倍：计算占比掩盖传输收益" % ratios[1],
                        (1, max(l1) * 2.0), ha="center", fontsize=8, color="#666666")
            ax.set_xticks(list(x), groups)
            ax.set_ylabel("每记录端到端耗时（纳秒，对数轴，越低越好）")
            ax.set_title("③ 端到端耗时（含计算）", fontsize=10)
            ax.legend(fontsize=7.5, loc="upper left")
            ax.set_ylim(15, max(l1) * 6)
        elif panel == "memory":
            jvm = _latest(con, "memory_engine_rss", "flink_jvm")[0]
            native = _latest(con, "memory_engine_rss", "flinkable")[0]
            ax.barh(["原版（JVM）", "flinkable"], [jvm, native], color=[GRAY, BLUE])
            ax.set_xlabel("引擎侧常驻内存 RSS 峰值（MB）")
            ax.annotate("−%.0f%%（微基准口径）" % (100.0 * (1 - native / jvm)),
                        (native * 1.8, 1), fontsize=8, color=BLUE)
            ax.set_title("④ 框架基础内存开销", fontsize=10)
            ax.invert_yaxis()
        elif panel == "throughput":
            enc = _latest(con, "framing_encode", "throughput")[0]
            dec = _latest(con, "framing_decode", "throughput")[0]
            take = _latest(con, "shuffle_take", "throughput")
            bars = ax.barh(["解码吞吐量", "编码吞吐量"], [dec, enc], color=BLUE, alpha=0.85)
            for bar in bars:
                ax.annotate("%.0f" % bar.get_width(),
                            (bar.get_width() * 1.01, bar.get_y() + bar.get_height() / 2),
                            va="center", fontsize=8)
            ax.invert_yaxis()
            note = "列式帧编解码吞吐量（MiB/s，定长 4 列）"
            if take:
                ax.set_title("⑤ 数据面吞吐（shuffle 随机取数 %.0f M 记录/秒）" % take[0], fontsize=9.5)
            ax.set_xlabel(note)
            ax.set_xlim(0, max(enc, dec) * 1.15)

    for ax in flat[len(panels):]:
        ax.axis("off")
    if trend:
        _draw_trend(fig, gs, rows - 1, trend)

    fig.text(0.5, 0.005,
             "数据来源：本地结果库（benchctl.py 摄取）；口径见报告 §1。端到端作业对比尚未开展（见报告覆盖矩阵）。",
             ha="center", fontsize=7.5, color="#666666")
    fig.tight_layout(rect=[0, 0.02, 1, 0.96])
    fig.savefig(out_path, dpi=160)
    return out_path

TREND_SERIES = [
    ("serde_string", "flinkable", "字符串 flinkable", BLUE, "o"),
    ("serde_string", "flink_jvm", "字符串 原版（JVM）", GRAY, "s"),
    ("serde_int_long", "flinkable", "定长字段 flinkable", "#2e9e57", "^"),
]


def _trend_series(con):
    """Codespeed-style history: run labels plus one series per tracked metric.

    A trend exists once any tracked series carries two or more ingested runs —
    the methodology's core artifact is the value-over-runs line, not the bar.
    """
    runs = con.execute(
        "SELECT id, source FROM runs WHERE suite != 'manual' ORDER BY id"
    ).fetchall()
    if not runs:
        return None
    series = []
    for case, metric, label, color, marker in TREND_SERIES:
        values = {}
        for rid, _src in runs:
            row = con.execute(
                "SELECT value FROM metrics WHERE run_id = ? AND case_name = ? AND metric = ?",
                (rid, case, metric),
            ).fetchone()
            if row:
                values[rid] = row[0]
        if len(values) >= 2:
            series.append((label, color, marker, values))
    if not series:
        return None
    return runs, series


def _draw_trend(fig, gs, row, trend):
    runs, series = trend
    ax = fig.add_subplot(gs[row, :])
    xs = list(range(len(runs)))
    labels = [src for (_rid, src) in runs]
    for label, color, marker, values in series:
        ys = [values.get(rid) for rid, _src in runs]
        pts = [(i, y) for i, (y, _r) in enumerate(zip(ys, runs)) if y is not None]
        if not pts:
            continue
        px, py = zip(*pts)
        ax.plot(px, py, marker=marker, color=color, label=label,
                linewidth=1.6, markersize=5)
        for i, y in zip(px, py):
            ax.annotate("%.0f" % y, (i, y), textcoords="offset points",
                        xytext=(0, 7), ha="center", fontsize=7.5, color=color)
        first, last = py[0], py[-1]
        if abs(last - first) / max(abs(first), 1e-9) > 0.15:
            arrow = "改善 %.0f%%" % (100.0 * (1 - last / first)) if last < first else "恶化 %.0f%%" % (
                100.0 * (last / first - 1))
            ax.annotate(arrow, (px[-1] + 0.12, last), fontsize=8.5, color=color,
                        fontweight="bold", va="center")
    ax.set_yscale("log")
    ax.set_xlim(xs[0] - 0.35, xs[-1] + 1.05)
    ax.set_xticks(xs, labels, fontsize=8)
    ax.set_ylabel("每记录耗时（纳秒，对数轴）")
    ax.set_title("⑥ 时间线趋势与回归标记（官方方法论核心视图：同一指标随摄取次序的走势）", fontsize=10)
    ax.legend(fontsize=8, loc="upper right")
    ax.grid(axis="y", which="both", alpha=0.25)
