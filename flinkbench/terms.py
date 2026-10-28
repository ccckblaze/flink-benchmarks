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
"""Display terminology — Flink documentation semantics only, no invented terms.

Each case/metric key stored by the ingesters maps to a Chinese label aligned
with the official Flink documentation wording and its English original.
Internal ledger slang never reaches a report through this table.
"""

CASE_LABELS = {
    "serde_int_long": "序列化往返：定长字段（整数+长整数）",
    "serde_string": "序列化往返：字符串（含构造）",
    "serde_string_full": "序列化往返：字符串（全链条：构造+编解码）",
    "serde_owned_form_int_long": "序列化往返（自有缓冲形态，历史基线）：定长字段",
    "serde_owned_form_string": "序列化往返（自有缓冲形态，历史基线）：字符串",
    "framing_encode": "列式帧编码（Apache Arrow）",
    "framing_decode": "列式帧解码（Apache Arrow）",
    "framing_wire": "线上字节量",
    "shuffle_take": "shuffle 随机取数",
    "embed_interface": "算子间数据传输（批接口）",
    "compute_arrow_native": "端到端耗时（向量化计算，Apache Arrow）",
    "compute_pandas": "端到端耗时（pandas 计算）",
    "operator_fusion": "算子融合（chaining）",
    "network_accounting": "网络缓冲记账",
    "cross_task_hop_floor": "跨任务通道往返下限",
    "memory_engine_rss": "引擎侧常驻内存 RSS",
    "bulk4_framing": "shuffle 序列化往返（定长 4 列记录）",
    "framing_coder": "列式帧编解码往返（Python 算子线格式）",
    "memory_jm_rss": "分布式部署常驻内存：JobManager（Java）",
    "memory_tm_rss": "分布式部署常驻内存：TaskManager",
    "backpressure_engine": "反压吸收（网络 shuffle 通道）",
    "backpressure_channel": "反压吸收：任务间通道停读",
    "backpressure_jvm_pool": "反压吸收：原版（缓冲池按预算封顶）",
    "backpressure_jvm_reserved": "反压吸收：原版（缓冲池可容纳全场景）",
    "backpressure_tcp_flow": "反压：网络通道·信用充足（写队列积压）",
    "backpressure_tcp_stall": "反压：网络通道·下游停读",
    "backpressure_tcp_stall_jvm": "反压：原版网络通道·下游停读（缓冲池封顶）",
    "backpressure_reserved_jvm": "反压：原版网络通道·下游停读（缓冲池预留全场景）",
    "backpressure_broadcast": "反压：广播扇出·一慢",
    "backpressure_broadcast_jvm": "反压：原版广播扇出·一慢",
    "backpressure_skew": "反压：数据倾斜·一热",
    "backpressure_skew_jvm": "反压：原版数据倾斜·一热",
    "backpressure_union": "反压：多输入归并·一慢",
    "backpressure_capture": "反压：非对齐检查点 × 在途数据",
    "backpressure_capture_jvm": "反压：原版非对齐检查点 × 在途数据",
    "backpressure_python": "反压：Python 算子停读（嵌入式）",
    "backpressure_tcp_stall_capped": "反压：网络通道·下游停读（生产者预算封顶）",
    "backpressure_channel_capped": "反压：任务间通道停读（预算封顶）",
    "backpressure_union_capped": "反压：多输入归并·一慢（预算封顶）",
    "memory_embedded_python": "嵌入式 Python 内存构成",
    "rss_base_mb": "基线常驻内存 RSS",
    "rss_queued_mb": "入队后常驻内存 RSS",
    "rss_peek_mb": "零拷贝捕获后常驻内存 RSS",
    "rss_peak_mb": "反压期间常驻内存 RSS（峰值）",
    "rss_after_mb": "排空后常驻内存 RSS",
    "backlog_mb": "队列/池驻留",
    "hot_mb": "热点通道驻留",
    "max_healthy_mb": "健康通道驻留上限",
    "captured_mb": "快照在途数据量",
    "drain_ms": "排空全部积压耗时",
    "engine_base_mb": "Rust 引擎基线",
    "interp_total_mb": "含 CPython 解释器",
    "libs_total_mb": "含 pyarrow/pandas 库",
    "batch_total_mb": "含在途 Arrow 批",
    "reservation_mb": "缓冲池预留量",
    "serde_shape_cjk10": "序列化往返：中文 10 字（非 ASCII）",
    "serde_string_codec": "序列化往返：字符串（仅编解码）",
    "serde_string_construct": "值构造项（基准开销，引擎不做构造）",
    "serde_shape_cjk40": "序列化往返：中文 40 字（非 ASCII）",
    "serde_shape_mixed": "序列化往返：中英混排 + emoji",
    "ab_encode_cjk_8": "编码通道：中文 10 字",
    "ab_encode_cjk_64": "编码通道：中文 52 字",
    "ab_encode_kana_16": "编码通道：日文 16 字",
    "ab_encode_emoji_5": "编码通道：emoji",
    "ab_encode_mixed_16": "编码通道：中英混排 + emoji",
    "ab_encode_ascii_12": "编码通道：ASCII 12 字节",
    "ab_encode_ascii_47": "编码通道：ASCII 47 字节",
    "ab_decode_cjk_8": "解码通道：中文 10 字",
    "ab_decode_cjk_64": "解码通道：中文 52 字",
    "ab_decode_kana_16": "解码通道：日文 16 字",
    "ab_decode_emoji_5": "解码通道：emoji",
    "ab_decode_mixed_16": "解码通道：中英混排 + emoji",
    "ab_decode_ascii_12": "解码通道：ASCII 12 字节",
    "ab_decode_ascii_47": "解码通道：ASCII 47 字节",
}

for _b in ("16", "128", "1024", "8192"):
    CASE_LABELS["smallbatch_" + _b] = "算子间数据传输（批大小 %s 行）" % _b

METRIC_LABELS = {
    "throughput": "吞吐量",
    "per_record": "每记录耗时",
    "per_cell": "每字段耗时",
    "bytes_per_record": "每记录线上字节",
    "flinkable": "flinkable（Rust 原生）",
    "flink_jvm": "Apache Flink 原版（JVM）",
    "flink_over_flinkable": "原版/flinkable 耗时比",
    "absorbed_mb": "反压期间引擎吸收（队列驻留）",
    "refused": "被拒写入次数（缓冲池背压信号）",
    "rss_stall_mb": "反压期间进程常驻内存增量",
    "rss_flow_mb": "畅通时进程常驻内存增量（对照）",
    "flow_buffered_mb": "畅通时引擎内缓冲（对照）",
    "produce_ns": "每缓冲写入耗时（畅通）",
    "stall_produce_ns": "每缓冲写入耗时（反压中）",
    "drain_ns": "恢复排空每记录耗时",
    "residual_mb": "恢复后队列残留",
    "rss_retained_mb": "恢复后常驻内存未回落",
    "flink_enc": "flinkable 编码段",
    "flink_dec": "flinkable 解码段",
    "pyflink": "原版 PyFlink（ArrowCoder，编+解）",
    "pyflink_enc": "原版 PyFlink 编码段（ArrowCoder）",
    "pyflink_dec": "原版 PyFlink 解码段（ArrowCoder）",
    "l2_zero_copy": "flinkable 引擎内零拷贝",
    "l1_ipc": "原版 PyFlink 进程间通道（Apache Arrow IPC）",
    "l2_end_to_end": "flinkable 端到端",
    "l1_end_to_end": "原版 PyFlink 端到端",
    "transfer_ratio": "传输耗时比（原版/flinkable）",
    "end_to_end_ratio": "端到端耗时比（原版/flinkable）",
    "export_only": "仅导出",
    "export_throughput": "导出吞吐量",
    "l2_throughput": "零拷贝吞吐量",
    "l1_throughput": "进程间通道吞吐量",
    "chained": "链式（未融合）",
    "fused": "融合后",
    "speedup": "加速比",
    "score": "得分",
    "error": "误差",
    "flinkable_before": "flinkable（改造前）",
    "flinkable_borrow": "flinkable（借用零分配）",
    "flinkable_reuse": "flinkable（复用目标对象）",
}

UNIT_LABELS = {
    "ns/row": "纳秒/记录",
    "ns/rec": "纳秒/记录",
    "ns/cell": "纳秒/字段",
    "MiB/s": "MiB/s",
    "Mrec/s": "百万记录/秒",
    "B/row": "字节/记录",
    "MB": "MB",
    "x": "倍",
    "ops/s": "次/秒",
    "rec/s": "记录/秒",
}


def case_label(case):
    return CASE_LABELS.get(case, case)


def metric_label(metric):
    return METRIC_LABELS.get(metric, metric)


def unit_label(unit):
    return UNIT_LABELS.get(unit, unit)
