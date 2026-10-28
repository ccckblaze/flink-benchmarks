# Flink 阶段一基线（flink-benchmarks JMH · 单核 · Flink 2.3.0）

## 测量环境（控制变量）

| 项 | 值 |
| --- | --- |
| Run ID | 20261028T092323Z |
| 机器 | VM-0-11-ubuntu |
| 时间 (UTC) | 2026-10-28T09:23:23Z |
| 系统 | Linux 7.0.0-30-generic |
| CPU | hypervisor hides the model (vCPU) |
| 架构 | x86_64 |
| 逻辑核数（配置） | 4 |
| 逻辑核数（在线） | 3 |
| 绑核 | 0 |
| 同物理核兄弟 | 0-1 |
| 兄弟核在线状态 | cpu0=1 (boot cpu) cpu1=0 |
| 主频 | 2545.218 |
| 调频策略 | n/a |
| C-state 内核参数 | intel_idle.max_cstate=1 intel_pstate=disable processor.max_cstate=1 |
| 透明大页 | always [madvise] never |
| 内存 | 14566 MB available / 659 MB free / 15480 MB total |
| 负载 | 0.52 0.30 0.17 1/221 787267 |
| 内存分配器 | libc.so.6 |
| Flink 版本 | 2.3.0 |
| jar | target/benchmarks.jar |
| 说明 | Stage-1 single-core throughput baseline; groups: window state-memory state-rocks |
| JMH 参数 | forks=1, warmup=2, iterations=7, iter_time=20s, xmx=4g, ActiveProcessorCount=1 (pinned) |
| Java | openjdk version "17.0.20.1" 2026-08-18 |
| 内核命令行 | `BOOT_IMAGE=/boot/vmlinuz-7.0.0-30-generic root=UUID=f8351c6b-9739-4e8d-abfd-fdbd167938da ro splash=silent showopts crashkernel=0M-3G:0M,3G-8G:192M,8G-16G:256M,16G-64G:384M,64G-128G:512M,128G-486G:768M,486G-972G:1024M,972G-:2048M net.ifnames=0 biosdevname=0 console=ttyS0,115200 console=tty0 panic=5 intel_idle.max_cstate=1 intel_pstate=disable processor.max_cstate=1 amd_iommu=on iommu=pt spec_rstack_overflow=off` |

## 结果

### state-memory

| 用例 | 模式 | 迭代数 | 得分 | 误差 (99.9%) | 单位 |
| --- | --- | --- | --- | --- | --- |
| stateBackends.FS | thrpt | 7 | 3404 | 52.0 | ops/ms |
| stateBackends.MEMORY | thrpt | 7 | 3640 | 94.5 | ops/ms |

### state-rocks

| 用例 | 模式 | 迭代数 | 得分 | 误差 (99.9%) | 单位 |
| --- | --- | --- | --- | --- | --- |
| stateBackends.ROCKS | thrpt | 7 | 3433 | 81.2 | ops/ms |
| stateBackends.ROCKS_INC | thrpt | 7 | 3499 | 81.5 | ops/ms |

### window

| 用例 | 模式 | 迭代数 | 得分 | 误差 (99.9%) | 单位 |
| --- | --- | --- | --- | --- | --- |
| globalWindow | thrpt | 7 | 5479 | 154.7 | ops/ms |
| sessionWindow | thrpt | 7 | 692 | 29.7 | ops/ms |
| slidingWindow | thrpt | 7 | 583 | 16.3 | ops/ms |
| tumblingWindow | thrpt | 7 | 3631 | 87.8 | ops/ms |
