# fabperf — 跨引擎性能实验（Stage 1：单机吞吐基线）

按 `flinkable/flink-native/docs/PERF_EXPERIMENT_PROPOSAL.md` 的方法学落地：
Flink 侧一律使用官方 flink-benchmarks 的 JMH 用例（不自造 Flink 探针），
在测量机上以「单物理核 + 关 SMT + 记录控制变量」的方式跑基线；
Flinkable 侧后续以同一批用例、同一台机器、同一套控制变量补测，再做对比。

## 文件

- `flink_baseline.sh` — 基线执行器：下线 SMT 兄弟核 → 写 `env.json` → 逐组跑 JMH → 输出 `csv/json/log`
- `env_snapshot.py` — 控制变量快照（CPU 型号、绑核、governor、THP、分配器、内核参数、内存、负载、JMH 参数）
- `summarize.py` — 把一次 run 目录整理成 `baseline.md`（环境表 + 分组结果表）
- `java/ClockCalib.java` — 有效主频标定（无 PMU 时折算周期/记录用），随每次 run 自动执行

## 用例组

| 组名 | JMH 类 | 用例 | 状态 |
| --- | --- | --- | --- |
| `window` | `WindowBenchmarks` | `globalWindow` / `tumblingWindow` / `slidingWindow` / `sessionWindow` | 有效区分项 |
| `cardinality` | `KeyedStateCardinalityBenchmark`（本 fork 新增） | `keyedState.<键基数>.<后端>`，1/1 千/10 万/100 万 × hashmap/rocksdb | **状态基线（修正组）** |
| `state-memory` | `MemoryStateBackendBenchmark` | `stateBackends.MEMORY` / `stateBackends.FS` | 已废：四腿同一后端，仅留作连续性记录 |
| `state-rocks` | `RocksStateBackendBenchmark` | `stateBackends.ROCKS` / `stateBackends.ROCKS_INC` | 已废：同上 |

单位 `ops/s`，越大越好（`ingest-jmh` 会按模式自动判定方向）。
`cardinality` 的用例名由 ingest 拼成 `keyedState.<numberOfKeys>.<stateBackend>`，
标签在 `flinkbench/terms.py` 里映射为文档口径名。

## 在测量机上跑一次基线

```bash
# 本地构建 jar（Java 字节码跨平台，无需在测量机上跑 Maven）
# 说明：Apache 快照仓库在国内直连只有个位数 KB/s，因此基线锁定已发布的
# Flink 2.3.0（走 Maven Central 镜像），产物为 target/benchmarks.jar。
cd flink-benchmarks
./mvnw -DskipTests -Dflink.version=2.3.0 -Drat.skip=true package

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
for g in window cardinality; do
  python3 benchctl.py ingest-jmh fabperf/results/<RUN_ID>/$g.csv \
      --host <box> --source fabperf-<RUN_ID>-$g
done
python3 fabperf/summarize.py fabperf/results/<RUN_ID>
```

## 可调变量（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `CORE` | `0` | 绑定的物理核；脚本自动下线其 SMT 兄弟核并在退出时恢复 |
| `FLINK_VERSION` | `2.3.0` | 基线 Flink 版本（flinkable 分支基于 2.3-SNAPSHOT，发布版为其后继） |
| `WARMUP` / `ITERATIONS` / `ITER_TIME` | `2` / `7` / `20s` | 提案要求 ≥7 次测量迭代，取 JMH 中位数与误差条 |
| `XMX` | `4g` | 单 fork JVM 堆 |
| `BENCH_GROUPS` | `window cardinality` | 可只跑子集；官方 state 两组已废，需要时显式列出 |
| `CLOCK_CALIB` | `1` | 置 `0` 跳过周期折算标定 |
| `CALIB_SECONDS` | `10` | 每种链长的标定秒数；`>=10` 时自校验差稳定在 0.3% 内 |
| `JAR` | 自动探测/构建 | 指定已构建 jar |
| `DATA_TMP` | `/mnt/datadisk0/fabperf/tmp` | RocksDB/临时文件放数据盘，避免污染系统盘 |
| `ENV_ONLY` | `0` | 置 `1` 只写环境快照 |

## 用例口径注记（读基线数字前必看）

- `state-memory` / `state-rocks` 四腿（`MEMORY`/`FS`/`ROCKS`/`ROCKS_INC`）**不是在测状态访问成本**，
  根因已定位到两层：
  1. **配置从未生效**：`StateBackendBenchmarkBase.StateBackendContext.setUp()` 里
     `Configuration configuration = Configuration.fromMap(...)` 设完 `STATE_BACKEND` /
     `CHECKPOINT_STORAGE` 后是局部变量，**从未应用到环境**（`checkpointDataUri` 同样未被使用）；
     而 `FlinkEnvironmentContext.setUp()` 又在 `createConfiguration()` **之后**硬设
     `clusterConfig.set(STATE_BACKEND, "hashmap")`。
  2. **即使写进作业配置也不够**：`DefaultExecutionGraphBuilder` 对状态后端的解析整段位于
     `else if (isCheckpointingEnabled(jobGraph))` 分支内，而
     `isCheckpointingEnabled(jobGraph)` 就是 `jobGraph.getCheckpointingSettings() != null`。
     该组不开检查点，解析路径根本不会走。
  因此四腿**从头到尾都在跑 HashMapStateBackend**，实测挤在 3400–3640 ops/ms（最大差 6.6%，
  落在 ±5% 噪声带边缘）是同一负载的重复测量，不是「后端成本接近」。
  另有一处上游自相矛盾：`MemoryStateBackendBenchmark` 用 7\,000\,000 条记录，
  `RocksStateBackendBenchmark` 用 2\,000\,000 条，两组之间本身也不可比较；键空间又被固定为
  `numberOfElements = 1000`，负载窗口是 10000 s 的滚动窗口且在测量期内永不触发。
- `cardinality`（修正组）怎么绕过这两层：**不改上游文件**（保住可合并性），在 fork 内新增
  `KeyedStateCardinalityBenchmark`，用 `env.configure()` 把 `state.backend` 写进**作业级**配置
  （`StateBackendLoader` 先并集群配置、再以作业配置覆盖），并打开 1 小时的检查点间隔——
  任何一次运行都触发不到屏障，`ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH=false` 也关掉收尾检查点，
  所以测量路径不变，只是让解析生效的开关。校验方式两种：setUp 内断言
  `StateBackendLoader.fromApplicationOrConfigOrDefault()` 解析出的类名；跑一次带 INFO 日志的验证腿，
  取引擎自己打印的 `State backend loader loads the state backend as …`，产物存 run 目录
  `backend_verification_<后端>.log`。**注意**：不要用「RocksDB 实例目录是否非空」来判定后端——
  作业结束时 Flink 会删除该目录，teardown 时永远看到空目录（本组初版就踩了这个坑）。
  验证结果（键基数 1000）：HashMapStateBackend 1073 ops/ms，EmbeddedRocksDBStateBackend 226 ops/ms，
  相差 4.7 倍——这才是官方四腿本该显示却完全没显示出来的差异。
- `window` 四腿是有效区分项：`globalWindow` 5479、`tumblingWindow` 3631、
  `slidingWindow` 583、`sessionWindow` 692 ops/ms（单核，Flink 2.3.0）。
  滑动/会话窗口的成本来自每记录多窗格写入，是 Flinkable 侧必须复刻的语义点。

## 每记录 CPU 周期：无 PMU 下的折算法（阶段一 §3.3 第二项的妥协方案）

`perf stat -e cycles,instructions` 在本机对两项**恒返回 140737488355327**（0x7FFFFFFFFFFFF，
即"未计数"哨兵值），`perf list hardware` 只有 `legacy hardware` 一组、无 `cache_references`
等硬件事件——该云虚机没有向客户机暴露 PMU，且 CPU 型号被 hypervisor 遮蔽为
`General Processors`，查不到 base/boost。因此**指令数与 IPC 测不出来**，必须换裸金属或确认
开放 PMU 的虚机才能补上（`perf stat -e cycles true` 应给出百万量级的真实计数）。

周期/记录退一步用**折算**：`java ClockCalib`（`fabperf/java/ClockCalib.java`）跑两条
loop-carried 依赖 ALU 链（2 指令版 `x=x^y; y=y+x;` 与 3 指令版 `x=x^y; y=y+x; x=x^y;`），
「每秒完成的指令数 ÷ 链长」即有效主频；两种链长互为自校验，若延迟模型不成立两者会分歧（`spread`）。
`flink_baseline.sh` 每次 run 自动在**同一绑核、同一 SMT 状态**下标定，写 `clockcalib.txt`，
report 用它把 ns/记录 换算为周期/记录并显式标注「折算」。 每次 run 自动在**同一绑核、同一 SMT 状态**下标定，写 `clockcalib.txt`，
report 用它把 ns/记录 换算为周期/记录并显式标注「折算」。

标定结果（run `20261028T114959Z` 及三次条件对照）：

| 条件 | 有效主频 | 两链自校验差 |
| --- | --- | --- |
| cpu0 独占、SMT 兄弟核下线（基线口径） | 3.2219 GHz | 0.01% |
| cpu0 独占、SMT 兄弟核下线（另一次独立标定） | 3.2208 GHz | 0.02% |
| cpu0 独占、SMT 兄弟核在线 | 3.2175 GHz | 0.01% |
| cpu0 + 另两个 vCPU 满载 | 3.1995 GHz | 0.26% |

三点含义：

- **可重复性好**：四次独立标定极差 0.7%，两种链长自校验差 ≤0.26%。
- `/proc/cpuinfo` 的 2545.218 MHz 是 hypervisor 公布的标称值，不是实际运行频率；基线口径下实跑
  约 3.22 GHz。整机满载只回落 0.7%，所以**折算周期/记录的随机不确定度约 ±1%**，量级可信。
- **但这是估计值不是计数值**：绝对精度依赖「整数 add/xor 为 1 周期延迟」这一 ISA 常识。试过用
  依赖乘法链（`imul` 3 周期）做外部对照，结论是**不可用**——C2 会把 `x = x * y` 循环整个消掉
  （实测 0.19 周期/迭代），而 `x = x * y + 1` 的速率与 2 指令 ALU 链相同、在本核乘法延迟未知
  （CPU 型号被遮蔽为 `General Processors`，查不到）时无法判别。所以两链一致只证明**内部自洽与
  可重复**，不证明延迟模型正确。报告一律写「折算」，要拿到真实周期数必须换有 PMU 的机器。

## 诚实性说明（写结论时必须带上）

- **单核结论**：Stage 1 只测单核算子吞吐，不代表集群整体扩展性；多核/网络/反压另议。
- **尾延迟未做**：阶段二需要核隔离的独占机器，当前测量机（4 vCPU 云主机）不满足条件，P99.9/P99.99 暂不出数。
- **崩溃恢复 MTTR 未做**：阶段三需要 dist 装配 + 控制面 failover + 活 JM 检查点协调（提案 §2 P-B），尚未解锁。
- **CPU 周期为折算、IPC 未做**：本机 PMU 不计数，周期/记录由标定主频折算（±1%），
  IPC 需指令计数无法折算，见上节。
- **崩溃恢复未做**：Stage 3 需要分布式部署 + failover 注入，尚未接入。
- **版本**：基线为 Flink `2.3.0` 发布版；flinkable 分支基线为 `2.3-SNAPSHOT`，两者同属 2.3 线，差异需在结论中注明。Flinkable 补测时必须是同一台机器、同一 `CORE`、同一 SMT 状态，否则不列入对比。
- **平手判定**：跨引擎差异 < ±5% 视为平手，不做「反超」渲染。
