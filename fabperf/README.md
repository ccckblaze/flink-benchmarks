# fabperf — 跨引擎性能实验（Stage 1：单机吞吐基线）

按 `flinkable/flink-native/docs/PERF_EXPERIMENT_PROPOSAL.md` 的方法学落地：
Flink 侧一律使用官方 flink-benchmarks 的 JMH 用例（不自造 Flink 探针），
在测量机上以「单物理核 + 关 SMT + 记录控制变量」的方式跑基线；
Flinkable 侧后续以同一批用例、同一台机器、同一套控制变量补测，再做对比。

## 文件

- `flink_baseline.sh` — 基线执行器：下线 SMT 兄弟核 → 写 `env.json` → 逐组跑 JMH → 输出 `csv/json/log`
- `env_snapshot.py` — 控制变量快照（CPU 型号、绑核、governor、THP、分配器、内核参数、内存、负载、JMH 参数）
- `summarize.py` — 把一次 run 目录整理成 `baseline.md`（环境表 + 分组结果表）

## 用例组

| 组名 | JMH 类 | 用例 |
| --- | --- | --- |
| `window` | `WindowBenchmarks` | `globalWindow` / `tumblingWindow` / `slidingWindow` / `sessionWindow` |
| `state-memory` | `MemoryStateBackendBenchmark` | `stateBackends.MEMORY` / `stateBackends.FS` |
| `state-rocks` | `RocksStateBackendBenchmark` | `stateBackends.ROCKS` / `stateBackends.ROCKS_INC` |

单位 `ops/s`，越大越好（`ingest-jmh` 会按模式自动判定方向）。

## 在测量机上跑一次基线

```bash
# 本地构建 jar（Java 字节码跨平台，无需在测量机上跑 Maven）
cd flink-benchmarks
./mvnw -DskipTests -Dflink.version=2.3-SNAPSHOT package

# 同步仓库 + jar 到测量机
rsync -az --exclude .git --exclude 'fabperf/results' ./ <box>:~/flink-benchmarks/
rsync -az target/flink-benchmarks-*.jar <box>:~/flink-benchmarks/target/

# 只校验环境快照，不跑测试
ssh <box> 'cd ~/flink-benchmarks && ENV_ONLY=1 ./fabperf/flink_baseline.sh'

# 正式跑（约 30–45 分钟，后台执行避免断连影响）
ssh <box> 'cd ~/flink-benchmarks && nohup ./fabperf/flink_baseline.sh > /tmp/fabperf.log 2>&1 &'
ssh <box> 'tail -f /tmp/fabperf.log'
```

跑完把结果取回本地并入库：

```bash
scp -r <box>:~/flink-benchmarks/fabperf/results/<RUN_ID> fabperf/results/
for g in window state-memory state-rocks; do
  python3 benchctl.py ingest-jmh fabperf/results/<RUN_ID>/$g.csv \
      --host <box> --source fabperf-<RUN_ID>-$g
done
python3 fabperf/summarize.py fabperf/results/<RUN_ID>
```

## 可调变量（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `CORE` | `0` | 绑定的物理核；脚本自动下线其 SMT 兄弟核并在退出时恢复 |
| `FLINK_VERSION` | `2.3-SNAPSHOT` | 与 flinkable 分支基线一致的 Flink 版本 |
| `WARMUP` / `ITERATIONS` / `ITER_TIME` | `2` / `7` / `20s` | 提案要求 ≥7 次测量迭代，取 JMH 中位数与误差条 |
| `XMX` | `4g` | 单 fork JVM 堆 |
| `GROUPS` | `window state-memory state-rocks` | 可只跑子集 |
| `JAR` | 自动探测/构建 | 指定已构建 jar |
| `DATA_TMP` | `/mnt/datadisk0/fabperf/tmp` | RocksDB/临时文件放数据盘，避免污染系统盘 |
| `ENV_ONLY` | `0` | 置 `1` 只写环境快照 |

## 诚实性说明（写结论时必须带上）

- **单核结论**：Stage 1 只测单核算子吞吐，不代表集群整体扩展性；多核/网络/反压另议。
- **尾延迟未做**：Stage 2 需要核隔离的独占机器，当前测量机（4 vCPU 云主机）不满足条件，P99.9/P99.99 暂不出数。
- **崩溃恢复未做**：Stage 3 需要分布式部署 + failover 注入，尚未接入。
- **版本**：基线为 Flink `2.3-SNAPSHOT`（flinkable 分支的基线版本）。Flinkable 补测时必须是同一台机器、同一 `CORE`、同一 SMT 状态，否则不列入对比。
- **平手判定**：跨引擎差异 < ±5% 视为平手，不做「反超」渲染。
