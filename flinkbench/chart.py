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


def _bar_labels(ax, bars, fmt="{:.0f}", fontsize=9):
    for bar in bars:
        ax.annotate(fmt.format(bar.get_height()),
                    (bar.get_x() + bar.get_width() / 2, bar.get_height()),
                    ha="center", va="bottom", fontsize=fontsize)


# M148 backpressure palette: one colour per component of the absolute RSS.
BP_PALE = "#c8ccd4"
BP_BLUE = BLUE
BP_PURPLE = "#7a5aa3"
BP_ORANGE = "#e8a33d"
BP_GOLD = "#c9a227"
BP_RED = "#b3403f"
BP_TEAL = "#2e8b8b"
BP_GREEN = "#2a7d4f"


def _bp_stacks(con, case, jvm):
    """Absolute-RSS decomposition of one backpressure leg.

    Returns (during_segments, after_segments, peak, refused); the segment
    lists stack bottom-up as (size, color, tag). None when the leg has not
    been ingested.
    """

    def _mb(metric):
        row = _latest(con, case, metric)
        return float(row[0]) if row is not None else None

    base, peak, after = _mb("rss_base_mb"), _mb("rss_peak_mb"), _mb("rss_after_mb")
    if base is None or peak is None:
        return None
    queued = _mb("rss_queued_mb")
    peek = _mb("rss_peek_mb")
    capped = case.endswith("_capped")
    held_color = BP_PURPLE if jvm else BP_BLUE
    held_tag = "缓冲池占用" if jvm else ("预算封顶积压" if capped else "引擎积压")
    if case == "backpressure_python" and _has(con, "memory_embedded_python"):
        eng = float(_latest(con, "memory_embedded_python", "engine_base_mb")[0])
        interp_tot = float(_latest(con, "memory_embedded_python", "interp_total_mb")[0])
        libs_tot = float(_latest(con, "memory_embedded_python", "libs_total_mb")[0])
        during = [(eng, BP_TEAL, "Flinkable 内核"),
                  (interp_tot - eng, BP_GOLD, "CPython 解释器"),
                  (libs_tot - interp_tot, BP_ORANGE, "Python 库"),
                  (max(base - libs_tot, 0.0), BP_PALE, "运行时基线"),
                  (peak - base, held_color, held_tag)]
    elif queued is not None:
        during = [(base, BP_PALE, "运行时基线"), (queued - base, held_color, held_tag)]
        if peek is not None:
            during.append((max(peek - queued, 0.0), BP_TEAL, "零拷贝捕获"))
            during.append((max(peak - peek, 0.0), BP_RED, "持久化拷贝"))
        else:
            during.append((max(peak - queued, 0.0), BP_RED, "快照额外拷贝"))
    else:
        during = [(base, BP_PALE, "运行时基线"), (peak - base, held_color, held_tag)]
    residue = max(after - base, 0.0) if after is not None else None
    after_segs = [(base, BP_PALE, "运行时基线")]
    if residue is not None and residue > 1.0:
        after_segs.append((residue, BP_RED, "未回落残留"))
    return during, after_segs, peak, _mb("refused")


def _bp_engine_tag(case, jvm):
    """Compact per-pair identity so adjacent bar pairs are distinguishable:
    engine (Flinkable / Flink) on the top line, buffer-budget state below.
    Flink's network budget is mandatory, so its "未设预算" is the reserved-pool
    proxy. The budget axis is dropped where it is not meaningful (capture /
    python / park compare a different variable)."""
    engine = "Flink" if (jvm or case.endswith("_jvm")) else "Flinkable"
    if case in ("backpressure_capture", "backpressure_capture_jvm",
                "backpressure_python", "backpressure_park", "backpressure_tcp_park"):
        return engine
    if case == "backpressure_reserved_jvm":
        budget = "未设预算"
    elif case.endswith("_capped"):
        budget = "设预算"
    elif jvm or case.endswith("_jvm"):
        budget = "设预算"
    else:
        budget = "未设预算"
    return engine + "\n" + budget


# M155: ⑦'s four paired shapes, drawn one-per-subplot on a full-width row.
# M153 kept only 设预算×设预算 pairs; 任务间通道停读 left the chart because
# Flink has no in-process task edge to pair it with (its counterpart is the
# network shape itself) — the leg stays in the report table.
BP_SHAPES = [
    ("网络通道停读", [
        ("backpressure_tcp_stall_capped", False),
        ("backpressure_tcp_stall_jvm", True)]),
    ("广播扇出·一慢", [
        ("backpressure_broadcast_capped", False),
        ("backpressure_broadcast_jvm", True)]),
    ("数据倾斜·一热", [
        ("backpressure_skew_capped", False),
        ("backpressure_skew_jvm", True)]),
    ("多输入归并·一慢", [
        ("backpressure_union_capped", False),
        ("backpressure_union_gates_jvm", True)]),
]
BP_TITLE = ("⑦ 缓冲队列类反压：双方均按预算运行\n"
            "左对柱＝Flinkable · 右对柱＝Flink｜对内：左=停读峰值 右=排空终值")


def _bp_panel(ax, con, groups, title, no_legend=False, legend_below=False,
              show_tags=True):
    # M148: one figure per backpressure family. Every engine inside a group
    # gets an adjacent pair of absolute-RSS bars — left bar is the peak while
    # the consumer is stalled, right bar is the RSS once it drained — so the
    # retention question answers itself at a glance. Bars stack by component.
    # M151 readability: each pair carries its engine tag (Rust/封顶/原版) in a
    # white box at the baseline, and a light divider separates one group from
    # the next, so "left-to-right, what am I looking at" is answerable without
    # the legend.
    from matplotlib.patches import Patch


    peak_max, used = 0.0, set()
    group_refused = {}
    stacks = {}
    for g, (label, engines) in enumerate(groups):
        present = [(c, j) for (c, j) in engines if _has(con, c)]
        stacks[g] = [(case, jvm, _bp_stacks(con, case, jvm)) for (case, jvm) in present]
    group_ceil = {g: max([st[2] for _c, _j, st in v if st] + [0.0])
                  for g, v in stacks.items()}
    for g, (label, _engines) in enumerate(groups):
        present = stacks[g]
        if not present:
            continue
        half = 0.8 / (2 * len(present))
        width = min(0.19, half * 0.92)
        for i, (case, jvm, st) in enumerate(present):
            if st is None:
                continue
            during, after_segs, peak, refused = st
            x0 = g - 0.4 + (2 * i + 0.5) * half
            x1 = g - 0.4 + (2 * i + 1.5) * half
            pair_mid = (x0 + x1) / 2.0
            bottom = 0.0
            for size, color, tag in during:
                if size <= 0:
                    continue
                ax.bar(x0, size, width, bottom=bottom, color=color)
                if size >= 12.0 and peak - (bottom + size / 2) > 14.0:
                    ax.text(x0, bottom + size / 2, "%d" % round(size), ha="center",
                            va="center", fontsize=6.5,
                            color="white" if color != BP_PALE else "#333")
                used.add((tag, color))
                bottom += size
            ax.text(x0, peak - 3, "%.0f" % round(peak), ha="center", va="top",
                    fontsize=7, color="white")
            if refused is not None:
                group_refused.setdefault(g, []).append(refused)
            top = sum(size for size, _c, _t in after_segs)
            bottom = 0.0
            for size, color, tag in after_segs:
                if size <= 0:
                    continue
                ax.bar(x1, size, width, bottom=bottom, color=color)
                if size >= 12.0 and top - (bottom + size / 2) > 14.0:
                    ax.text(x1, bottom + size / 2, "%d" % round(size), ha="center",
                            va="center", fontsize=6.5,
                            color="white" if color != BP_PALE else "#333")
                used.add((tag, color))
                bottom += size
            if len(after_segs) > 1:
                if after_segs[1][0] < 0.5 * peak:
                    ax.text(x1, top + 4, "残留 %.0f" % round(after_segs[1][0]),
                            ha="center", fontsize=6.5, color=BP_RED)
            else:
                ax.text(x1, top + 4, "已回落", ha="center", fontsize=6.5, color=BP_GREEN)
            if show_tags:
                tag = _bp_engine_tag(case, jvm)
                ax.annotate(tag, (pair_mid, 0.0), xycoords=("data", "axes fraction"),
                            xytext=(0, 3), textcoords="offset points", ha="center", va="bottom",
                            fontsize=6.5, fontweight="bold",
                            color=BP_PURPLE if (jvm or case.endswith("_jvm")) else BLUE,
                            bbox=dict(boxstyle="round,pad=0.18", fc="white", ec="#bbb", lw=0.5,
                                      alpha=0.92))
            peak_max = max(peak_max, peak)
    for g in range(1, len(groups)):
        ax.axvline(g - 0.5, color="#e2e2e2", lw=0.8, zorder=0)
    # M157: one merged 拒绝 label per group (left bar / right bar order),
    # above the group's tallest bar — per-bar labels collided horizontally.
    present_n = {g: len(v) for g, v in stacks.items()}
    for g, values in group_refused.items():
        n = max(present_n.get(g, 1), 1)
        center = g - 0.4 + 0.8 / 2.0
        text = "拒绝 " + " · ".join(str(int(v)) for v in values)
        ax.text(center, group_ceil[g] * 1.06, text, ha="center", fontsize=6.5,
                color=BP_BLUE if any(values) else BP_RED)
    order = ["运行时基线", "Flinkable 内核", "CPython 解释器", "Python 库", "引擎积压",
             "预算封顶积压", "缓冲池占用", "零拷贝捕获", "持久化拷贝", "快照额外拷贝", "未回落残留"]
    handles = [Patch(color=c, label=t) for t in order for (tag, c) in used if tag == t]
    ax.set_xticks(range(len(groups)), [g[0] for g in groups])
    ax.tick_params(axis="x", labelsize=6.5)
    ax.set_ylabel("进程常驻内存 RSS（MB，绝对值）", fontsize=8)
    ax.set_title(title, fontsize=8.5, pad=40 if legend_below else 4)
    legend_corner = "upper right" if peak_max < 300 else "upper left"
    if legend_below:
        # M157: at cell width the under-axis legend collided with the shape
        # tick labels; expand it above the axes (the title sits one line
        # higher, so the band between title and axes is free).
        ax.legend(handles=handles, fontsize=6, loc="lower left",
                  bbox_to_anchor=(0.0, 1.005, 1.0, 0.16), mode="expand",
                  ncol=4, framealpha=1.0, columnspacing=0.9, handlelength=1.2,
                  handletextpad=0.4)
    elif not no_legend:
        ax.legend(handles=handles, fontsize=7, loc=legend_corner, ncol=2)
    ax.set_ylim(0, peak_max * 1.30)


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
    if _has(con, "bulk4_framing") and _has(con, "framing_coder"):
        panels.append("shuffle")
    elif _has(con, "framing_encode") and _has(con, "framing_decode"):
        panels.append("throughput")
    if _has(con, "serde_shape_cjk10"):
        panels.append("shapes")
    if _has(con, "backpressure_tcp_stall"):
        panels.append("backpressure")
        panels.append("backpressure_release")

    n = len(panels)
    if n == 0:
        raise SystemExit("no panel data in the store — ingest a run first")
    trend = _trend_series(con)
    # M156: the whole comparison is one screen — eight panels on a 4-column
    # grid (two rows of four), the trend line spans the full width below.
    cols = 4
    panel_rows = (n + cols - 1) // cols
    rows = panel_rows + (1 if trend else 0)
    fig = plt.figure(figsize=(22.0, 4.6 * panel_rows + (3.6 if trend else 0)))
    gs = GridSpec(rows, cols, figure=fig, hspace=0.85, wspace=0.30)
    flat = [fig.add_subplot(gs[i // cols, i % cols]) for i in range(n)]
    fig.suptitle(title, fontsize=15, fontweight="bold", y=0.99)
    w = 0.34

    for idx, (ax, panel) in enumerate(zip(flat, panels)):
        if panel == "serde":
            # M130: the headline is the whole chain — construction, encode and decode in
            # one lane, engine memory reuse included. No term splits, no cherry-picking:
            # this is the same shape the JVM leg (fresh String per record, both ends) pays.
            if _has(con, "serde_string_full"):
                cases = ["serde_int_long", "serde_string_full"]
                groups = ["定长字段\n（整数+长整数）", "字符串\n（全链条）"]
            else:
                cases = ["serde_int_long", "serde_string"]
                groups = ["定长字段\n（整数+长整数）", "字符串\n（含构造）"]
            native = [_latest(con, c, "flinkable")[0] for c in cases]
            jvm = [_latest(con, c, "flink_jvm")[0] for c in cases]
            ratios = [_latest(con, c, "flink_over_flinkable") for c in cases]
            x = range(len(groups))
            b1 = ax.bar([i - w / 2 for i in x], native, w, color=BLUE, label="Flinkable")
            b2 = ax.bar([i + w / 2 for i in x], jvm, w, color=GRAY, label="Flink")
            _bar_labels(ax, list(b1) + list(b2))
            for i, ratio in enumerate(ratios):
                if not ratio:
                    continue
                word = "Flinkable 快 %.2f 倍" % ratio[0] if ratio[0] >= 1 else "Flinkable 慢 %.2f 倍" % (1 / ratio[0])
                # The empty column between the two bars, at the mean of their tops: a
                # shared top row collides with the legend, a per-bar row with the value labels.
                ax.annotate(word, (i, max(native[i], jvm[i]) * 1.48), ha="center", fontsize=9,
                            color=BLUE if ratio[0] >= 1 else "#a33",
                            bbox=dict(boxstyle="round,pad=0.18", fc="white", ec="none", alpha=0.85))
            ax.set_xticks(list(x), groups)
            ax.set_ylabel("每记录往返耗时（纳秒，越低越好）")
            ax.set_title("① 序列化 / 反序列化往返（同一会话并列）\n"
                         "字节格式与 Flink 原版完全一致 · 逐字节校验", fontsize=9)
            ax.legend(fontsize=8, loc="upper left")
            ax.set_ylim(0, max(native + jvm) * 1.66)
        elif panel == "smallbatch":
            batches = ["16", "128", "1024", "8192"]
            l2 = [_latest(con, "smallbatch_" + b, "l2_zero_copy")[0] for b in batches]
            l1 = [_latest(con, "smallbatch_" + b, "l1_ipc")[0] for b in batches]
            ratios = [_latest(con, "smallbatch_" + b, "transfer_ratio")[0] for b in batches]
            x = range(len(batches))
            ax.bar([i - w / 2 for i in x], l2, w, color=BLUE,
                   label="Flinkable 引擎内零拷贝（Apache Arrow）")
            ax.bar([i + w / 2 for i in x], l1, w, color=GRAY,
                   label="Flink PyFlink 进程间通道（Apache Arrow IPC）")
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
            # Both bars compute on Arrow columnar batches; what differs is the kernel and
            # the channel: arrow-rs in-process zero-copy vs pyarrow behind Arrow IPC.
            groups = ["向量化计算\n（Arrow 列式）", "pandas 计算\n（Arrow 列式喂入）"]
            cases = ["compute_arrow_native", "compute_pandas"]
            l2 = [_latest(con, c, "l2_end_to_end")[0] for c in cases]
            l1 = [_latest(con, c, "l1_end_to_end")[0] for c in cases]
            ratios = [_latest(con, c, "end_to_end_ratio")[0] for c in cases]
            x = range(len(groups))
            ax.bar([i - w / 2 for i in x], l2, w, color=BLUE,
                   label="Flinkable（arrow-rs 内核，进程内零拷贝）")
            ax.bar([i + w / 2 for i in x], l1, w, color=GRAY,
                   label="Flink PyFlink（pyarrow 内核，Arrow IPC 进程间）")
            ax.set_yscale("log")
            _bar_labels(ax, [bar for container in ax.containers for bar in container], "{:.1f}")
            ax.annotate("端到端 %.2f 倍" % ratios[0], (0, l1[0] * 1.45), ha="center", fontsize=9, color=BLUE)
            ax.annotate("%.2f 倍：计算占比掩盖传输收益" % ratios[1],
                        (1, l1[1] * 1.45), ha="center", fontsize=8, color="#666666")
            ax.set_xticks(list(x), groups)
            ax.set_ylabel("每记录端到端耗时（纳秒，对数轴，越低越好）")
            ax.set_title("③ 端到端耗时（含计算）", fontsize=10)
            ax.legend(fontsize=7.5, loc="upper left")
            ax.set_ylim(15, max(l1) * 6)
        elif panel == "shapes":
            # M146: same shape, same session, same discipline on both bars — the canonical
            # leg pair printed by the cross-engine table run (Rust lane beside the real
            # Flink StringSerializer round trip). The before/after history lives in the
            # ledger and the report table, not on the comparison chart.
            specs = [("serde_shape_cjk10", "中文 10 字"),
                     ("serde_shape_cjk40", "中文 40 字"),
                     ("serde_shape_mixed", "中英混排\n+ emoji")]
            specs = [(c, g) for c, g in specs if _has(con, c)]
            after = [_latest(con, c, "flinkable")[0] for c, _g in specs]
            jvm = [_latest(con, c, "flink_jvm")[0] for c, _g in specs]
            groups = [g for _c, g in specs]
            x = range(len(groups))
            b2 = ax.bar([i - w / 2 for i in x], after, w, color=BLUE, label="Flinkable")
            b3 = ax.bar([i + w / 2 for i in x], jvm, w, color=GRAY, label="Flink")
            _bar_labels(ax, list(b2) + list(b3))
            for i in x:
                ratio = jvm[i] / after[i]
                word = "Flinkable 快 %.2f 倍" % ratio if ratio >= 1 else "Flinkable 慢 %.2f 倍" % (1 / ratio)
                ax.annotate(word, (i, max(after[i], jvm[i]) * 1.16), ha="center", fontsize=9,
                            color=BLUE if ratio >= 1 else "#a33",
                            bbox=dict(boxstyle="round,pad=0.18", fc="white", ec="none", alpha=0.85))
            ax.set_xticks(list(x), groups)
            ax.tick_params(axis="x", labelsize=8)
            ax.set_ylabel("每记录往返耗时（纳秒，越低越好）")
            ax.set_title("⑥ 非 ASCII 字符串编解码（与 Flink 同会话并列）", fontsize=10)
            ax.legend(fontsize=7.5, loc="upper right")
            ax.set_ylim(0, max(after + jvm) * 1.42)
        elif panel == "backpressure":
            _bp_panel(ax, con, BP_SHAPES, BP_TITLE, legend_below=True,
                       show_tags=False)
        elif panel == "backpressure_release":
            _bp_panel(ax, con, [
                ("非对齐检查点\n（在途数据快照）", [("backpressure_capture", False),
                                        ("backpressure_capture_jvm", True)]),
                ("Python 算子停读\n（Flinkable 嵌入式）", [("backpressure_python", False)]),
            ], "⑧ 检查点与 Python 算子：排空前 vs 排空后的绝对 RSS\n"
            "左对柱＝Flinkable · 右对柱＝Flink（仅一对时为 Flinkable）｜对内：左=峰值 右=终值",
            no_legend=True, show_tags=False)
        elif panel == "memory":
            if _has(con, "memory_jm_rss"):
                # M131: the standardized distributed deployment — one JobManager plus one
                # TaskManager running WordCount over a 5.5 GB data-disk input, medians of
                # twenty RSS samples per process. The Java JM is the same process on both
                # sides (the control plane is kept), so the delta rides entirely on the TM.
                # M132: the third bar is the ORIGINAL PyFlink deployment — the same
                # Flink 2.3.0 cluster running the distribution's own word_count.py,
                # where every slot adds a separate Python worker process on top of
                # the Java TM (and the TM itself grows for the python bridge).
                orange = "#e8a33d"
                gold = "#c9a227"
                jm_jvm = _latest(con, "memory_jm_rss", "flink_jvm")[0]
                tm_jvm = _latest(con, "memory_tm_rss", "flink_jvm")[0]
                jm_fab = _latest(con, "memory_jm_rss", "flinkable")[0]
                tm_fab = _latest(con, "memory_tm_rss", "flinkable")[0]
                has_py = _has(con, "memory_py_rss")
                has_fab_py = con.execute(
                    "SELECT 1 FROM metrics WHERE case_name = 'memory_tm_rss' AND metric = 'flinkable_py' LIMIT 1"
                ).fetchone() is not None
                if has_fab_py:
                    tm_fab_py = _latest(con, "memory_tm_rss", "flinkable_py")[0]
                if has_py:
                    jm_py = _latest(con, "memory_jm_rss", "flink_pyflink")[0]
                    tm_py = _latest(con, "memory_tm_rss", "flink_pyflink")[0]
                    py_py = _latest(con, "memory_py_rss", "flink_pyflink")[0]
                    # M159: like-for-like adjacency — the Java pair (Flink vs
                    # Flinkable Java job) and the Python pair (PyFlink vs
                    # Flinkable Python job) sit next to each other so each
                    # delta is read inside its own workload group.
                    cats = ["Flink\n（Java）", "Flinkable\n（Java）",
                            "Flink\n（PyFlink）", "Flinkable\n（Python）"] if has_fab_py else \
                           ["Flink\n（Java）", "Flinkable\n（Java）", "Flink\n（PyFlink）"]
                    tops = [jm_jvm, jm_fab] + ([jm_py, jm_fab] if has_fab_py else [jm_py])
                    mids = [tm_jvm, tm_fab] + ([tm_py, tm_fab_py] if has_fab_py else [tm_py])
                    bots = [0.0, 0.0] + ([py_py, 0.0] if has_fab_py else [py_py])
                    # M148: the flinkable Python interpreter lives inside the TM process,
                    # so its bar decomposes along the embedded probe's RSS steps: Rust
                    # runtime, the CPython interpreter floor, the pyarrow/pandas library
                    # floor, and whatever Python-side cost is left over. The bar total is
                    # exactly what the cluster measured — only the split comes from the probe.
                    fab_py_split = None
                    if has_fab_py and _has(con, "memory_embedded_python"):
                        eng = float(_latest(con, "memory_embedded_python", "engine_base_mb")[0])
                        interp_tot = float(_latest(con, "memory_embedded_python", "interp_total_mb")[0])
                        libs_tot = float(_latest(con, "memory_embedded_python", "libs_total_mb")[0])
                        interp_share = interp_tot - eng
                        libs_share = libs_tot - interp_tot
                        other_share = max(tm_fab_py - tm_fab - interp_share - libs_share, 0.0)
                        fab_py_split = (interp_share, libs_share, other_share)
                        mids[3] = tm_fab
                        bots[3] = interp_share
                    totals = [t + m + b for t, m, b in zip(tops, mids, bots)]
                    if fab_py_split is not None:
                        totals[3] += fab_py_split[1] + fab_py_split[2]
                    ax.bar(cats, tops, 0.55, color=orange,
                           label="JobManager")
                    ax.bar(cats, mids, 0.55, bottom=tops,
                           color=[GRAY, BLUE, GRAY, BLUE][:len(cats)],
                           label="TaskManager")
                    ax.bar(cats, bots, 0.55,
                           bottom=[t + m for t, m in zip(tops, mids)],
                           color=gold,
                           label="Python 解释器")
                    if fab_py_split is not None:
                        interp_share, libs_share, other_share = fab_py_split
                        light = "#c8ccd4"
                        top3 = tops[3] + mids[3] + interp_share
                        ax.bar([3], [libs_share], 0.55, bottom=[top3], color=orange,
                               label="Python 库")
                        ax.bar([3], [other_share], 0.55, bottom=[top3 + libs_share], color=light,
                               label="其它 Python 开销")
                    if has_fab_py:
                        ax.axvline(1.5, color="#e2e2e2", lw=0.8, zorder=0)
                    for x, (t, m) in enumerate(zip(tops, mids)):
                        ax.annotate("JM %.0f" % t, (x, t / 2), ha="center", fontsize=8, color="white")
                        if x == 3 and fab_py_split is not None:
                            interp_share, libs_share, other_share = fab_py_split
                            base3 = t + m
                            py_tot = interp_share + libs_share + other_share
                            ax.annotate("PY %.0f" % py_tot, (x, base3 + py_tot / 2),
                                        ha="center", va="center", fontsize=8, color="white")
                        elif x == 3:
                            ax.annotate("TM %.0f" % m, (x, t + m / 2), ha="center", fontsize=8, color="white")
                        else:
                            ax.annotate("TM %.0f" % m, (x, t + m / 2), ha="center", fontsize=8, color="white")
                    if has_py and bots[2] > 0:
                        ax.annotate("PY %.0f" % bots[2], (2, tops[2] + mids[2] + bots[2] / 2),
                                    ha="center", fontsize=8, color="white")
                else:
                    cats = ["Apache Flink 原版", "flinkable"]
                    tops, mids = [jm_jvm, jm_fab], [tm_jvm, tm_fab]
                    totals = [t + m for t, m in zip(tops, mids)]
                    ax.bar(cats, tops, 0.5, color=orange,
                           label="JobManager（Java，两侧同源）")
                    ax.bar(cats, mids, 0.5, bottom=tops,
                           color=[GRAY, BLUE], label="TaskManager")
                    for x, (t, m) in enumerate(zip(tops, mids)):
                        ax.annotate("JM %.0f" % t, (x, t / 2), ha="center", fontsize=8, color="white")
                        if x < len(cats) - 1:
                            ax.annotate("TM %.0f" % m, (x, t + m / 2), ha="center", fontsize=8, color="white")
                # M158: the percentage rides on the Flinkable bars' totals —
                # no floating summary block, no leader-line annotations.
                for x, tot in enumerate(totals):
                    if has_py and x == 1:
                        txt = "%.0f MB\nTM %.1f · −%.0f%%" % (tot, mids[1],
                                                               100.0 * (1 - tot / totals[0]))
                    elif has_py and x == 3:
                        txt = "%.0f MB\n较 PyFlink −%.0f%%" % (tot, 100.0 * (1 - tot / totals[2]))
                    elif not has_py and x == 1:
                        txt = "%.0f MB\n较 Flink −%.0f%%" % (tot, 100.0 * (1 - tot / totals[0]))
                    else:
                        txt = "%.0f MB" % tot
                    lx = x - (0.10 if x == len(totals) - 1 else 0)
                    ax.annotate(txt, (lx, tot), ha="center", va="bottom",
                                xytext=(0, 3 if "\n" not in txt else 8),
                                textcoords="offset points", fontsize=8.5,
                                color=BLUE if "\n" in txt else "black")
                ax.set_ylabel("常驻内存 RSS（MB，越低越好）")
                ax.set_title("④ 分布式部署内存（1 JM + 1 TM，WordCount 5.5 GB 运行中）\n"
                             "同作业成对对比：左对＝Java 作业 · 右对＝Python 作业",
                             fontsize=9.5, pad=56)
                # M157: legend above the axes with short labels — the long
                # per-process descriptions collided in two columns at cell
                # width (they live in the report table now).
                ax.legend(fontsize=6.5, loc="lower left",
                          bbox_to_anchor=(0.0, 1.005, 1.0, 0.30), mode="expand",
                          ncol=3, framealpha=1.0, columnspacing=0.9,
                          handlelength=1.2, handletextpad=0.4)
                ax.set_ylim(0, max(totals) * 1.22)
            else:
                jvm = _latest(con, "memory_engine_rss", "flink_jvm")[0]
                native = _latest(con, "memory_engine_rss", "flinkable")[0]
                ax.barh(["Flink", "Flinkable"], [jvm, native], color=[GRAY, BLUE])
                ax.set_xlabel("引擎侧常驻内存 RSS 峰值（MB）")
                ax.annotate("−%.0f%%（微基准口径）" % (100.0 * (1 - native / jvm)),
                            (native * 1.8, 1), fontsize=8, color=BLUE)
                ax.set_title("④ 框架基础内存开销", fontsize=10)
                ax.invert_yaxis()
        elif panel == "shuffle":
            # M131: the honest counterpart of the columnar framing wire is the encoder the
            # original PyFlink worker runs on the SAME contract (one self-contained IPC
            # stream per batch) — not Flink's row serializer, which prices a different
            # wire entirely (that leg stays in the tables as a diagnostic).
            total = _latest(con, "bulk4_framing", "flinkable")[0]
            enc = _latest(con, "bulk4_framing", "flink_enc")[0]
            dec = _latest(con, "bulk4_framing", "flink_dec")[0]
            py = _latest(con, "framing_coder", "pyflink")[0]
            b1 = ax.bar([-w / 2], [total], w, color=BLUE, label="Flinkable（arrow-rs 编码/解码）")
            b2 = ax.bar([w / 2], [py], w, color=GRAY, label="Flink PyFlink（pyarrow ArrowCoder）")
            _bar_labels(ax, list(b1) + list(b2), "{:.1f}")
            ax.annotate("编码 %.1f + 解码 %.1f" % (enc, dec), (-w / 2, total * 0.45),
                        ha="center", fontsize=8, color="white")
            ratio = py / total
            word = "Flinkable 快 %.2f 倍" % ratio if ratio >= 1 else "Flinkable 慢 %.2f 倍" % (1 / ratio)
            ax.annotate(word, (0, (total + py) / 2), ha="center", fontsize=9,
                        color=BLUE if ratio >= 1 else "#a33",
                        bbox=dict(boxstyle="round,pad=0.18", fc="white", ec="none", alpha=0.85))
            ax.set_xticks([0], ["定长 4 列记录\n（每记录 32 字节）"])
            ax.set_ylabel("每记录往返耗时（纳秒，越低越好）")
            title5 = "⑤ 列式帧编解码往返（Python 算子线格式：同一契约两引擎）"
            if _has(con, "serde_bulk4_count"):
                # M132: the production pump never full-decodes — it only needs
                # the row total, now counted from the message headers. The
                # count lane rides in the title: the plot body stays collision-free.
                cnt = _latest(con, "serde_bulk4_count", "flinkable")[0]
                title5 = ("⑤ 列式帧编解码往返（同一契约两引擎）\n"
                          "泵计数道 %.2f ns/记录 · 全解码的 1/%.0f" % (cnt, total / cnt))
            ax.set_title(title5, fontsize=9.5)
            ax.legend(fontsize=7.5, loc="upper left")
            ax.set_ylim(0, max(total, py) * 1.3)
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
        _draw_trend(fig, gs, panel_rows, trend)

    fig.text(0.5, 0.005,
             "数据来源：本地结果库（benchctl.py 摄取）；口径见报告 §1。端到端作业对比尚未开展（见报告覆盖矩阵）。",
             ha="center", fontsize=7.5, color="#666666")
    fig.tight_layout(rect=[0, 0.02, 1, 0.96])
    fig.savefig(out_path, dpi=160)
    return out_path

TREND_SERIES = [
    # Every flinkable series carries a same-colour original-Flink (JVM)
    # reference line: dashed = reference (no point labels, no delta arrow).
    ("serde_string", "flinkable", "字符串 Flinkable", BLUE, "o", "-"),
    ("serde_string", "flink_jvm", "字符串 Flink", GRAY, "s", "-"),
    ("serde_int_long", "flinkable", "定长字段 Flinkable", "#2e9e57", "^", "-"),
    ("serde_int_long", "flink_jvm", "定长字段 Flink", "#2e9e57", "s", "--"),
    ("serde_string_full", "flinkable", "字符串全链条 Flinkable", "#7a5cc4", "D", "-"),
    ("serde_string_full", "flink_jvm", "字符串全链条 Flink", "#7a5cc4", "s", "--"),
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
    for case, metric, label, color, marker, style in TREND_SERIES:
        values = {}
        for rid, _src in runs:
            row = con.execute(
                "SELECT value FROM metrics WHERE run_id = ? AND case_name = ? AND metric = ?",
                (rid, case, metric),
            ).fetchone()
            if row:
                values[rid] = row[0]
        if len(values) >= 2:
            series.append((label, color, marker, style, values))
    if not series:
        return None
    # Only runs that actually carry a point belong on the axis, and a milestone
    # ingested in two passes (the M147 backpressure session is one) must not
    # repeat its label — suffix the duplicates so the axis stays readable.
    used = {rid for (_label, _color, _marker, _style, values) in series for rid in values}
    runs = [(rid, src) for (rid, src) in runs if rid in used]
    total = {}
    for _rid, src in runs:
        total[src] = total.get(src, 0) + 1
    seen, labelled = {}, []
    for rid, src in runs:
        if total[src] == 1:
            labelled.append((rid, src))
            continue
        seen[src] = seen.get(src, 0) + 1
        labelled.append((rid, "%s\u00b7%s" % (src, chr(ord("a") + seen[src] - 1))))
    return labelled, series


def _draw_trend(fig, gs, row, trend):
    runs, series = trend
    ax = fig.add_subplot(gs[row, :])
    xs = list(range(len(runs)))
    labels = [src for (_rid, src) in runs]
    for label, color, marker, style, values in series:
        ys = [values.get(rid) for rid, _src in runs]
        pts = [(i, y) for i, (y, _r) in enumerate(zip(ys, runs)) if y is not None]
        if not pts:
            continue
        px, py = zip(*pts)
        ax.plot(px, py, marker=marker, color=color, label=label,
                linewidth=1.6, markersize=5, linestyle=style)
        if style == "--":
            continue
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
    ax.set_title("⑨ 时间线趋势与回归标记（官方方法论核心视图：同一指标随摄取次序的走势）", fontsize=10)
    # M157: legend below the axes — above it collided with the panel title;
    # the end-of-line 改善/恶化 markers stay clear in the right margin.
    ax.legend(fontsize=7.5, loc="upper left", bbox_to_anchor=(0.0, -0.14, 1.0, 0.24),
              mode="expand", ncol=3, framealpha=1.0)
    ax.grid(axis="y", which="both", alpha=0.25)


# --- stage-1 single-core baseline (official flink-benchmarks JMH) -------------
STAGE1_GROUPS = [
    ("窗口分配器（Window Assigners）｜最快与最慢相差 9.4 倍，是有效区分项",
     ["globalWindow", "tumblingWindow", "slidingWindow", "sessionWindow"], BLUE, None),
    ("状态后端与检查点存储｜四腿只差 6.6%，读数不反映状态访问",
     ["stateBackends.MEMORY", "stateBackends.FS", "stateBackends.ROCKS",
      "stateBackends.ROCKS_INC"], GRAY,
     "上游 StateBackendBenchmarkBase 把键基数固定为 1000（numberOfElements=1000）、窗口是\n"
     "10000 s 的滚动窗口且在测量期内不触发、迭代期间没有检查点，四腿量的是 source→map→\n"
     "keyBy→window 管道，不是状态访问成本。跨引擎对比前须两侧同时放大键基数并打开检查点。"),
]

# What the proposal asks for in section 3.3, and whether this box can deliver it.
# No ✓/✗ here on purpose: PingFang SC has no U+2713/U+2717 and they render as tofu.
STAGE1_COVERAGE = (
    "提案 §3.3 三项指标覆盖：① 单核吞吐【已测】（本图，按 §3.3 折算为 Mops/s）；"
    "② 每记录 CPU 周期 / IPC【未测】—— 本机 PMU 不计数，`perf stat` 对 cycles/instructions "
    "恒返回 140737488355327，需裸金属或开放 PMU 的虚机；"
    "③ 窗口触发频率 / 状态读写放大【未测】—— 官方 JMH 用例不暴露算子度量，需在 harness 侧打点。")

# JMH reports ops/ms; the proposal reports Mops/s (section 3.3).
UNIT_TO_MOPS = {"ops/ms": 1.0 / 1000.0, "ops/s": 1.0 / 1e6}


def stage1_cases(con):
    """Cases whose latest value came from an official JMH (stage-1) run."""
    return {row[0] for row in con.execute(
        """SELECT DISTINCT m.case_name FROM metrics m
           JOIN runs r ON r.id = m.run_id WHERE r.suite = 'jmh'""")}


def render_stage1(con, out_path, run_note=""):
    """The Flink-only stage-1 baseline: bars with JMH error bars, per group.

    Deliberately not merged into the comparison figure - these cases have one
    leg only, and a lone Flink bar next to an absent Flinkable bar would read
    as a verdict the control variables do not support.
    """
    from .terms import case_short_label

    cases = stage1_cases(con)
    drawn = [group for group in STAGE1_GROUPS if any(c in cases for c in group[1])]
    if not drawn:
        return None

    fig, axes = plt.subplots(1, len(drawn), figsize=(7.0 * len(drawn), 5.2))
    axes = [axes] if len(drawn) == 1 else list(axes)
    for ax, (heading, wanted, color, warning) in zip(axes, drawn):
        names = [c for c in wanted if c in cases]
        values, errors, scale = [], [], 1.0
        for case in names:
            score = _latest(con, case, "score")
            error = _latest(con, case, "error")
            scale = UNIT_TO_MOPS.get(score[1], 1.0)
            values.append(score[0] * scale)
            errors.append(0.0 if error is None else error[0] * scale)
        ypos = range(len(names))
        ax.barh(ypos, values, height=0.62, color=color, alpha=0.9,
                xerr=errors,
                error_kw={"ecolor": "#444444", "elinewidth": 1.1, "capsize": 3})
        for y, value, error in zip(ypos, values, errors):
            ax.annotate("%.2f ±%.2f" % (value, error),
                        (value + error, y), xytext=(6, 0), textcoords="offset points",
                        va="center", fontsize=9, color="#222222")
        ax.set_yticks(list(ypos), [case_short_label(c) for c in names], fontsize=9.5)
        ax.invert_yaxis()
        ax.set_xlim(0, max(values) * 1.34)
        ax.set_xlabel("单核吞吐（百万操作/秒 Mops/s，越高越好）", fontsize=9)
        ax.set_title(heading, fontsize=10, loc="left", pad=8)
        ax.grid(axis="x", alpha=0.25)
        ax.set_axisbelow(True)
        # The note lives in a reserved empty band under the last bar; drawing
        # it at the bottom of the axes overlapped the last case's value label.
        if warning:
            note, note_color, face, edge, reserve = (
                warning, "#8a2a2a", "#fdf1f1", "#d9a2a2", 1.95)
        else:
            note, note_color, face, edge, reserve = (
                "Flinkable 同形腿：待补（提案 §2 P-A：`fab-windows` 窗口分配器、类型化记录通路）",
                "#3066be", "#eef3fb", "#b7c8e8", 1.25)
        ax.text(0.015, 0.015, note, transform=ax.transAxes, fontsize=8.0,
                va="bottom", ha="left", color=note_color,
                bbox=dict(boxstyle="round,pad=0.35", facecolor=face,
                          edgecolor=edge, alpha=0.95))
        ax.set_ylim(len(names) - 0.5 + reserve, -0.5)
        ax.margins(x=0)

    fig.suptitle("阶段一｜单核「内存到内存」吞吐基线（Flink 原版，官方 flink-benchmarks JMH）",
                 fontsize=13, fontweight="bold", y=0.99)
    if run_note:
        fig.text(0.5, 0.932, run_note, ha="center", fontsize=8.8, color="#555555")
    fig.text(0.5, 0.015, STAGE1_COVERAGE, ha="center", va="bottom", fontsize=7.8,
             color="#666666", wrap=True)
    fig.tight_layout(rect=(0, 0.085, 1, 0.905))
    fig.savefig(out_path, dpi=160)
    plt.close(fig)
    return out_path
