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
# 报告快照

`benchctl report` 的产物按里程碑归档在此，便于对照走势；`flinkbench/out/` 是工作目录，不入库。

| 快照 | 内容 |
| --- | --- |
| `2026-10-27-m128/` | 七面板同屏对比：序列化往返、Python 通道往返、端到端、内存开销、数据面吞吐、非 ASCII 编解码（同会话 A/B）、时间线趋势与回归标记 |

重新生成：`python3 benchctl.py --db flinkbench/bench.db report --out flinkbench/out --env "..." --env "..."`
