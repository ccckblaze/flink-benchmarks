<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# flinkbench：以 flink-benchmarks 方法论为基准的二开套件

## 上游方法论 → 本套件对应物

| 上游 flink-benchmarks | 本套件 | 说明 |
| --- | --- | --- |
| `benchmark.sh`（JMH 薄壳，`-foe true -rf csv`） | 原样沿用 | JVM 基线仍走 JMH，产物 `jmh-result.csv` |
| `save_jmh_result.py` → Codespeed 服务器 | `benchctl.py ingest-jmh` → 本地 sqlite | 去服务器化；方向语义照抄（thrpt 越高越好，avgt/ss 越低越好；Param 列折进用例名） |
| （无对应物——上游只有 JVM） | `benchctl.py ingest-measured` | flinkable 台子的 MEASURED / P57-* 日志摄取，47+ 指标一跑入册 |
| `regression_report_v2.py`（基线窗口 + 稳定度阈值） | `flinkbench/regression.py` + `benchctl.py check` | 算法移植：对最近基线窗最佳值回退超 max(最小比例, 倍数×末段相对标准差) 判回退；样本 <5 次不判 |
| Codespeed 网页 | `benchctl.py report` → `report.md` + `report.png` | 独立实现报告+生图；术语只走 Flink 官方文档语义（`terms.py`），台账俚语不入图 |

## 用法

```bash
# JVM 基线（上游跑法不变）
mvn clean install exec:exec                       # 或 java -jar target/benchmarks.jar -rf csv "<类>"
python3 benchctl.py ingest-jmh jmh-result.csv --host <机器名> --source <版本/提交>

# flinkable 原生腿（台子 release，--ignored 显式召出）
# 台子到本地线路慢，远端先过滤再拉（全量日志可达 20 MB+）：
ssh -p 22 <rig> "grep -E 'MEASURED|P57-|ratio |L2 |L1 |export-only' /tmp/<log>" > /tmp/<log>.filtered
python3 benchctl.py ingest-measured /tmp/<log>.filtered --host <机器名> --source <里程碑>

# 单值登记（如内存口径）
python3 benchctl.py put memory_engine_rss flinkable 4 MB --source P57

# 报告 + 生图 / 回归检查 / 历史
python3 benchctl.py report --out flinkbench/out --env "环境：<一句话>"
python3 benchctl.py check          # 有回退则退出码 1，可挂 CI
python3 benchctl.py runs
```

结果库默认 `flinkbench/bench.db`（sqlite，不入库）；报告默认 `flinkbench/out/`（不入库）。

## canonical 对表会话（跨引擎面板的唯一入册来源）

一条会话 = 台子上按顺序跑完下列腿，日志汇聚后先过门、再入册：

```bash
# 台子（洁净机，--test-threads=1，release）
cargo test --release -p fab-serde --test jvm_serializer_bench -- --ignored --test-threads=1 --nocapture
cargo test --release -p fab-serde --test string_breakdown m128_vs_m126_same_session -- --ignored --test-threads=1 --nocapture
cargo test --release -p fab-serde --test serde_bench -- --ignored --test-threads=1 --nocapture
cargo test --release -p fab-embed -- --ignored --test-threads=1 --nocapture
cargo test --release -p fab-arrow --test data_plane_throughput -- --ignored --test-threads=1 --nocapture
cargo test --release -p fab-task --test perf_bench -- --ignored --test-threads=1 --nocapture
~/.fabvenvs/fabpyflink311/bin/python ~/flink-native/tools/py-coder-bench/coder_bench.py

# 本地：过滤 → 过带 → 入册 → 出报告
ssh -p 22 <rig> "grep -E 'MEASURED|P57-|P58-|^AB\\||ratio |L2 |L1 |export-only|pyflink coder' /tmp/<log>" > /tmp/<log>.filtered
python3 benchctl.py gate-jvm /tmp/<log>.filtered --exclude-source <里程碑>   # 非 0 退出 = JVM 腿出带，禁止入册
python3 benchctl.py ingest-measured /tmp/<log>.filtered --host rig --source <里程碑>
python3 benchctl.py report --out flinkbench/out --env "环境：<一句话>"
```

**漂移纪律（M137 先例，`gate-jvm` 已固化）**：跨引擎比值面板的 JVM 腿带着自身机器态
（JIT/GC 对字符串形状的敏感度），历史观测非 ASCII 形状腿可摆动 25-30% 而定形腿不动。
出带会话若入册，会把 Rust 呈现成“对着更快的 JVM 回退了”——那是参照引擎在动，不是本引擎。
故：`gate-jvm` 以**最近三次**同腿值的中位数为带（一次重新锚定后旧制度不投票）、
被审会话自身不参与锚带，超 ±15% 即 FAIL；FAIL 的会话只入台账、不入册、不覆盖既有面板。
收益改由**同一二进制内的 A/B 腿**（`AB|` 行）与未改动腿作漂移对照来入账。

## 设计口径（与上游一致的三条）

1. **单机微基准**：所有数字声明为微基准口径，端到端作业对比未开展前不出端到端结论；
2. **方向感知**：每个指标随值记录 `lower_is_better` / `higher_is_better`，报告与回归判定共用；
3. **来源可溯**：每个 run 记 host + suite + source（里程碑/提交），图上数值永远取该用例最近一次 run。

## 测试

```bash
python3 -m unittest discover -s flinkbench/tests -t .
```

依赖：Python 3.9+（标准库 sqlite3/csv）；生图另需 matplotlib + 中文字体（macOS 走 PingFang SC）。
