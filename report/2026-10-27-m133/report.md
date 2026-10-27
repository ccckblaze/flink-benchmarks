# flinkable × Apache Flink 关键指标同屏对比（benchctl 生成）

环境：bench rig（x86_64 4 核 Linux、数据盘、release 构建），锚栈 CPython 3.11.17 + pyarrow 14.0.2 + beam 2.54.0 + pemja 0.5.7，JDK 17
分布式内存用例：Apache Flink 2.3.0 发行版 standalone 集群（1 JM + 1 TM，默认配置）；PyFlink 柱为同一集群跑发行版自带 word_count.py（含每槽位 Python worker 进程）；flinkable Python 作业柱为内嵌解释器（Rust+CPython+pyarrow+pandas 单进程）实测中位

![关键指标同屏对比](report.png)

## 指标明细（每项取最近一次摄取值）

| 指标 | 度量 | 数值 | 方向 |
| --- | --- | --- | --- |
| 序列化往返：定长字段（整数+长整数） | Apache Flink 原版（JVM） | 13 纳秒/记录 | 越低越好 |
| 序列化往返：定长字段（整数+长整数） | 原版/flinkable 耗时比 | 2.6 倍 | 越高越好 |
| 序列化往返：定长字段（整数+长整数） | flinkable（Rust 原生） | 5 纳秒/记录 | 越低越好 |
| 序列化往返：字符串（含构造） | Apache Flink 原版（JVM） | 74 纳秒/记录 | 越低越好 |
| 序列化往返：字符串（含构造） | 原版/flinkable 耗时比 | 0.77 倍 | 越高越好 |
| 序列化往返：字符串（含构造） | flinkable（Rust 原生） | 96 纳秒/记录 | 越低越好 |
| 算子间数据传输（批大小 16 行） | 原版 PyFlink 进程间通道（Apache Arrow IPC） | 1062 纳秒/记录 | 越低越好 |
| 算子间数据传输（批大小 16 行） | flinkable 引擎内零拷贝 | 270.2 纳秒/记录 | 越低越好 |
| 算子间数据传输（批大小 16 行） | 传输耗时比（原版/flinkable） | 3.93 倍 | 越高越好 |
| 算子间数据传输（批大小 128 行） | 原版 PyFlink 进程间通道（Apache Arrow IPC） | 130.1 纳秒/记录 | 越低越好 |
| 算子间数据传输（批大小 128 行） | flinkable 引擎内零拷贝 | 30.9 纳秒/记录 | 越低越好 |
| 算子间数据传输（批大小 128 行） | 传输耗时比（原版/flinkable） | 4.21 倍 | 越高越好 |
| 算子间数据传输（批大小 1024 行） | 原版 PyFlink 进程间通道（Apache Arrow IPC） | 22.3 纳秒/记录 | 越低越好 |
| 算子间数据传输（批大小 1024 行） | flinkable 引擎内零拷贝 | 3.5 纳秒/记录 | 越低越好 |
| 算子间数据传输（批大小 1024 行） | 传输耗时比（原版/flinkable） | 6.35 倍 | 越高越好 |
| 算子间数据传输（批大小 8192 行） | 原版 PyFlink 进程间通道（Apache Arrow IPC） | 7.7 纳秒/记录 | 越低越好 |
| 算子间数据传输（批大小 8192 行） | flinkable 引擎内零拷贝 | 0.4 纳秒/记录 | 越低越好 |
| 算子间数据传输（批大小 8192 行） | 传输耗时比（原版/flinkable） | 17.65 倍 | 越高越好 |
| 端到端耗时（向量化计算，Apache Arrow） | 端到端耗时比（原版/flinkable） | 1.83 倍 | 越高越好 |
| 端到端耗时（向量化计算，Apache Arrow） | 原版 PyFlink 端到端 | 48.1 纳秒/记录 | 越低越好 |
| 端到端耗时（向量化计算，Apache Arrow） | flinkable 端到端 | 26.2 纳秒/记录 | 越低越好 |
| 端到端耗时（pandas 计算） | 端到端耗时比（原版/flinkable） | 1.03 倍 | 越高越好 |
| 端到端耗时（pandas 计算） | 原版 PyFlink 端到端 | 803.2 纳秒/记录 | 越低越好 |
| 端到端耗时（pandas 计算） | flinkable 端到端 | 778 纳秒/记录 | 越低越好 |
| 引擎侧常驻内存 RSS | Apache Flink 原版（JVM） | 406 MB | 越低越好 |
| 引擎侧常驻内存 RSS | flinkable（Rust 原生） | 4 MB | 越低越好 |
| 列式帧编码（Apache Arrow） | 每记录耗时 | 18.42 纳秒/记录 | 越低越好 |
| 列式帧编码（Apache Arrow） | 吞吐量 | 1716 MiB/s | 越高越好 |
| 列式帧解码（Apache Arrow） | 每字段耗时 | 4.075 纳秒/字段 | 越低越好 |
| 列式帧解码（Apache Arrow） | 每记录耗时 | 16.3 纳秒/记录 | 越低越好 |
| 列式帧解码（Apache Arrow） | 吞吐量 | 1939 MiB/s | 越高越好 |
| 线上字节量 | 每记录线上字节 | 33.14 字节/记录 | 越低越好 |
| shuffle 随机取数 | 每字段耗时 | 0.762 纳秒/字段 | 越低越好 |
| shuffle 随机取数 | 每记录耗时 | 3.05 纳秒/记录 | 越低越好 |
| shuffle 随机取数 | 吞吐量 | 328.1 百万记录/秒 | 越高越好 |
| 算子间数据传输（批接口） | 仅导出 | 0.6 纳秒/记录 | 越低越好 |
| 算子间数据传输（批接口） | 导出吞吐量 | 5.124e+04 MiB/s | 越高越好 |
| 算子间数据传输（批接口） | 原版 PyFlink 进程间通道（Apache Arrow IPC） | 26.3 纳秒/记录 | 越低越好 |
| 算子间数据传输（批接口） | 进程间通道吞吐量 | 1186 MiB/s | 越高越好 |
| 算子间数据传输（批接口） | 零拷贝吞吐量 | 1.807e+04 MiB/s | 越高越好 |
| 算子间数据传输（批接口） | flinkable 引擎内零拷贝 | 1.7 纳秒/记录 | 越低越好 |
| 算子间数据传输（批接口） | 传输耗时比（原版/flinkable） | 15.24 倍 | 越高越好 |
| 解码通道：ASCII 12 字节 | flinkable（Rust 原生） | 35 纳秒/记录 | 越低越好 |
| 解码通道：ASCII 12 字节 | flinkable（改造前） | 32 纳秒/记录 | 越低越好 |
| 解码通道：ASCII 12 字节 | flinkable（借用零分配） | 22 纳秒/记录 | 越低越好 |
| 解码通道：ASCII 47 字节 | flinkable（Rust 原生） | 37 纳秒/记录 | 越低越好 |
| 解码通道：ASCII 47 字节 | flinkable（改造前） | 47 纳秒/记录 | 越低越好 |
| 解码通道：ASCII 47 字节 | flinkable（借用零分配） | 24 纳秒/记录 | 越低越好 |
| 解码通道：中文 52 字 | flinkable（Rust 原生） | 212 纳秒/记录 | 越低越好 |
| 解码通道：中文 52 字 | flinkable（改造前） | 398 纳秒/记录 | 越低越好 |
| 解码通道：中文 52 字 | flinkable（借用零分配） | 212 纳秒/记录 | 越低越好 |
| 解码通道：中文 10 字 | flinkable（Rust 原生） | 56 纳秒/记录 | 越低越好 |
| 解码通道：中文 10 字 | flinkable（改造前） | 130 纳秒/记录 | 越低越好 |
| 解码通道：中文 10 字 | flinkable（借用零分配） | 60 纳秒/记录 | 越低越好 |
| 解码通道：emoji | flinkable（Rust 原生） | 41 纳秒/记录 | 越低越好 |
| 解码通道：emoji | flinkable（改造前） | 54 纳秒/记录 | 越低越好 |
| 解码通道：emoji | flinkable（借用零分配） | 44 纳秒/记录 | 越低越好 |
| 解码通道：日文 16 字 | flinkable（Rust 原生） | 71 纳秒/记录 | 越低越好 |
| 解码通道：日文 16 字 | flinkable（改造前） | 172 纳秒/记录 | 越低越好 |
| 解码通道：日文 16 字 | flinkable（借用零分配） | 74 纳秒/记录 | 越低越好 |
| 解码通道：中英混排 + emoji | flinkable（Rust 原生） | 65 纳秒/记录 | 越低越好 |
| 解码通道：中英混排 + emoji | flinkable（改造前） | 172 纳秒/记录 | 越低越好 |
| 解码通道：中英混排 + emoji | flinkable（借用零分配） | 68 纳秒/记录 | 越低越好 |
| 编码通道：ASCII 12 字节 | flinkable（Rust 原生） | 8 纳秒/记录 | 越低越好 |
| 编码通道：ASCII 12 字节 | flinkable（改造前） | 7 纳秒/记录 | 越低越好 |
| 编码通道：ASCII 47 字节 | flinkable（Rust 原生） | 9 纳秒/记录 | 越低越好 |
| 编码通道：ASCII 47 字节 | flinkable（改造前） | 9 纳秒/记录 | 越低越好 |
| 编码通道：中文 52 字 | flinkable（Rust 原生） | 208 纳秒/记录 | 越低越好 |
| 编码通道：中文 52 字 | flinkable（改造前） | 343 纳秒/记录 | 越低越好 |
| 编码通道：中文 10 字 | flinkable（Rust 原生） | 45 纳秒/记录 | 越低越好 |
| 编码通道：中文 10 字 | flinkable（改造前） | 67 纳秒/记录 | 越低越好 |
| 编码通道：emoji | flinkable（Rust 原生） | 26 纳秒/记录 | 越低越好 |
| 编码通道：emoji | flinkable（改造前） | 36 纳秒/记录 | 越低越好 |
| 编码通道：日文 16 字 | flinkable（Rust 原生） | 60 纳秒/记录 | 越低越好 |
| 编码通道：日文 16 字 | flinkable（改造前） | 97 纳秒/记录 | 越低越好 |
| 编码通道：中英混排 + emoji | flinkable（Rust 原生） | 76 纳秒/记录 | 越低越好 |
| 编码通道：中英混排 + emoji | flinkable（改造前） | 89 纳秒/记录 | 越低越好 |
| shuffle 序列化往返（定长 4 列记录） | flinkable 解码段 | 4 纳秒/记录 | 越低越好 |
| shuffle 序列化往返（定长 4 列记录） | flinkable 编码段 | 11 纳秒/记录 | 越低越好 |
| shuffle 序列化往返（定长 4 列记录） | Apache Flink 原版（JVM） | 9 纳秒/记录 | 越低越好 |
| shuffle 序列化往返（定长 4 列记录） | 原版/flinkable 耗时比 | 0.6 倍 | 越低越好 |
| shuffle 序列化往返（定长 4 列记录） | flinkable（Rust 原生） | 15 纳秒/记录 | 越低越好 |
| 列式帧编解码往返（Python 算子线格式） | 原版 PyFlink（ArrowCoder，编+解） | 11.04 纳秒/记录 | 越低越好 |
| 列式帧编解码往返（Python 算子线格式） | 原版 PyFlink 解码段（ArrowCoder） | 2.1 纳秒/记录 | 越低越好 |
| 列式帧编解码往返（Python 算子线格式） | 原版 PyFlink 编码段（ArrowCoder） | 8.94 纳秒/记录 | 越低越好 |
| 分布式部署常驻内存：JobManager（Java） | Apache Flink 原版（JVM） | 349 MB | 越低越好 |
| 分布式部署常驻内存：JobManager（Java） | 原版/flinkable 耗时比 | 1 倍 | 越高越好 |
| 分布式部署常驻内存：JobManager（Java） | flink_pyflink | 345.5 MB | 越低越好 |
| 分布式部署常驻内存：JobManager（Java） | flinkable（Rust 原生） | 349 MB | 越低越好 |
| memory_py_rss | flink_pyflink | 191.9 MB | 越低越好 |
| 分布式部署常驻内存：TaskManager | Apache Flink 原版（JVM） | 692 MB | 越低越好 |
| 分布式部署常驻内存：TaskManager | 原版/flinkable 耗时比 | 187 倍 | 越高越好 |
| 分布式部署常驻内存：TaskManager | flink_pyflink | 780 MB | 越低越好 |
| 分布式部署常驻内存：TaskManager | flinkable（Rust 原生） | 3.7 MB | 越低越好 |
| 分布式部署常驻内存：TaskManager | flinkable_py | 140.4 MB | 越低越好 |
| serde_bulk4_count | flinkable（Rust 原生） | 0.302 ns_per_record | 越低越好 |
| 序列化往返：中文 10 字（非 ASCII） | Apache Flink 原版（JVM） | 99 纳秒/记录 | 越低越好 |
| 序列化往返：中文 10 字（非 ASCII） | 原版/flinkable 耗时比 | 1.09 倍 | 越高越好 |
| 序列化往返：中文 10 字（非 ASCII） | flinkable（Rust 原生） | 91 纳秒/记录 | 越低越好 |
| 序列化往返：中文 40 字（非 ASCII） | Apache Flink 原版（JVM） | 281 纳秒/记录 | 越低越好 |
| 序列化往返：中文 40 字（非 ASCII） | 原版/flinkable 耗时比 | 1.03 倍 | 越高越好 |
| 序列化往返：中文 40 字（非 ASCII） | flinkable（Rust 原生） | 274 纳秒/记录 | 越低越好 |
| 序列化往返：中英混排 + emoji | Apache Flink 原版（JVM） | 162 纳秒/记录 | 越低越好 |
| 序列化往返：中英混排 + emoji | 原版/flinkable 耗时比 | 1.23 倍 | 越高越好 |
| 序列化往返：中英混排 + emoji | flinkable（Rust 原生） | 132 纳秒/记录 | 越低越好 |
| 序列化往返：字符串（仅编解码） | Apache Flink 原版（JVM） | 61 纳秒/记录 | 越低越好 |
| 序列化往返：字符串（仅编解码） | 原版/flinkable 耗时比 | 1.15 倍 | 越高越好 |
| 序列化往返：字符串（仅编解码） | flinkable（Rust 原生） | 53 纳秒/记录 | 越低越好 |
| 值构造项（基准开销，引擎不做构造） | Apache Flink 原版（JVM） | 13 纳秒/记录 | 越低越好 |
| 值构造项（基准开销，引擎不做构造） | 原版/flinkable 耗时比 | 0.29 倍 | 越高越好 |
| 值构造项（基准开销，引擎不做构造） | flinkable（Rust 原生） | 45 纳秒/记录 | 越低越好 |
| 序列化往返：字符串（全链条：构造+编解码） | Apache Flink 原版（JVM） | 74 纳秒/记录 | 越低越好 |
| 序列化往返：字符串（全链条：构造+编解码） | 原版/flinkable 耗时比 | 1.64 倍 | 越高越好 |
| 序列化往返：字符串（全链条：构造+编解码） | flinkable（Rust 原生） | 45 纳秒/记录 | 越低越好 |

## 非 ASCII 字符串编解码（同会话 A/B，改造前后与原版并列）

| 形状 | 改造前 | 改造后 | 提速 | 原版（JVM） | 改造后/原版 |
| --- | --- | --- | --- | --- | --- |
| 中文 10 字 | 197 | 101 | 49% | 99 纳秒/记录 | 0.98 倍 |
| 中文 52 字 | 741 | 420 | 43% | 未对表 | — |
| 日文 16 字 | 269 | 131 | 51% | 未对表 | — |
| emoji | 90 | 67 | 26% | 未对表 | — |
| 中英混排 + emoji | 261 | 141 | 46% | 162 纳秒/记录 | 1.15 倍 |
| ASCII 12 字节（对照） | 39 | 43 | -10% | 未对表 | — |

口径：编解码往返耗时（纳秒/记录），不含值构造项；“改造前/改造后”取自同一次运行内的 A/B 腿，避免跨会话机器状态漂移；“原版”为 Flink `StringSerializer` 经 `DataOutputSerializer`/`DataInputDeserializer` 的完整往返（另含一次缓冲拷贝分配）。emoji 与日文行暂无原版对表值。

## 时间线趋势（同一指标随摄取次序，Codespeed 式核心视图）

| 指标 | 度量 | M124 | M125 | M126·a | M126·b | M126·c | M126·d | M128 | M129 | M130 | M131 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 序列化往返：字符串（含构造） | flinkable（Rust 原生） | — | 245 | 149 | 138 | 87 | 90 | 105 | 99 | 110 | 96 |
| 序列化往返：字符串（含构造） | Apache Flink 原版（JVM） | — | 59 | 60 | 58 | 58 | 64 | 71 | 72 | 72 | 74 |
| 序列化往返：定长字段（整数+长整数） | flinkable（Rust 原生） | — | 22 | 5 | 5 | 5 | 5 | 5 | 5 | 5 | 5 |
| 序列化往返：定长字段（整数+长整数） | Apache Flink 原版（JVM） | — | 13 | 13 | 13 | 14 | 13 | 13 | 13 | 13 | 13 |

## 回归检查（基线窗口算法，移植自上游 regression_report_v2）

| 指标 | 度量 | 状态 |
| --- | --- | --- |
| 序列化往返：定长字段（整数+长整数） | Apache Flink 原版（JVM） | 正常 |
| 序列化往返：定长字段（整数+长整数） | flinkable（Rust 原生） | 正常 |
| 序列化往返：字符串（含构造） | Apache Flink 原版（JVM） | 回退 24.1%（阈值 5.0%） |
| 序列化往返：字符串（含构造） | flinkable（Rust 原生） | 正常 |

另有 92 项序列摄取次数不足 5 次，按上游方法论不参与回归判定，仅计入趋势面板。

## 术语对照

| 报告用词 | Flink 文档对应 |
| --- | --- |
| 序列化 / 反序列化 | Serialization / Deserialization |
| 每记录耗时 | per-record latency |
| 批大小 | batch size |
| 零拷贝 | zero-copy（Apache Arrow） |
| 进程间通道 | inter-process data plane（Beam worker，Arrow IPC） |
| 常驻内存（RSS） | Resident Set Size |
| 改造前 / 改造后 | before / after（同一二进制内的 A/B 腿） |
| 借用零分配 | zero-copy decode，借用输入缓冲不新建字符串 |
| 值构造项 | 基准里构造输入对象的费用，非引擎开销 |
| 同会话 | 同一台机器、同一次运行内并列测量（跨会话不可比） |
| 非 ASCII 形状 | 中文 / 日文 / emoji（UTF-16 码元多字节形制） |
| 吞吐量 | throughput |
| shuffle 随机取数 | shuffle read（Arrow take） |

