# 联邦负载安置与撤离庭

离线多集群控制面模拟，Python 3.11+ 标准库与 SQLite 即可运行。它只处理法域合同、集群席位和故障接管，不连接 Kubernetes 或云 API。

## 三个入口

- `intent`：租户法域规则、负载合同与主站授权。合同冻结保留既有负载，但阻止新席位；授权代次决定谁能指定主站。
- `seat`：集群资源向量、灾难域和主备席位。并发预留在 SQLite IMMEDIATE 事务中结算，旧观测、同灾难域和重复主备角色均阻断。
- `evac`：事故等级、撤离计划和双端交接回执。事故可从故障处置证据派生等级，或回退到调用者给定的严重度；只有源端围栏与目标端就绪的当前集群代次均成立，才转移主站并释放旧席位。
- `timeline`：多源故障重建。传感器原件、跨集群物理拓扑、时钟更正与事故假设共同决定原因、处置等级和客户中断窗口。

`timeline` 维护 `fault_samples/fault_clock_revisions/fault_topology/fault_evidence_versions/fault_dispositions/fault_seats/fault_takeovers/fault_outages/fault_move_evidence/fault_counters/fault_verdicts`。它从原件重建带不确定区间的观测，按物理拓扑（运营商段、电力馈线）区分共享段故障与并行独立故障，并把原因写成带范围的确定/待证/被反证假设。时钟更正只追加修订；事故当时的客户处置与告警保留不变，迟到反证产生新证据版本并要求对已启动搬迁作出取消、继续或补偿裁决。

这三个入口分别维护 `workload_intents/residency_rules/owner_grants`、`cluster_inventory/capacity_claims/placement_leases`、`incident_cases/move_plans/handoff_receipts`。每条命令保存原始输入、裁决及业务表，可从事件顺序重放并逐表核对。

## 运行

在仓库根目录：

```sh
python -m cloudcourt demo --db demo.sqlite
python -m cloudcourt replay --db demo.sqlite
python -m unittest discover -s tests -v
```

`fixtures/<入口>-normal.json` 和 `fixtures/<入口>-failure.json` 分别给出正常与改变业务结论的反例；`fixtures/path.json` 在席位就绪后指定原主站，`demo` 再运行事故接管。`fixtures/fault-cross-cluster/normal.json` 是跨集群乱序传感器 fixture：两个集群共用运营商段但电力馈线独立，传感器带时钟偏差与伪造/重放上传，覆盖时钟更正、迟到反证和客户中断窗口。独立入口命令如 `python -m cloudcourt seat --db state.sqlite --fixture fixtures/seat-normal.json`。首次运行请使用新数据库文件。

关键交互：法域合同限制候选集群；主备席位的灾难域与资源占用改变撤离计划；双端回执缺失或目标换代时，旧主站仍保持不变。测试覆盖并发容量、部分交接及目标重建。

目标换代使旧交接回执失效时，`evac abort` 会保留恢复案、释放旧目标备席，再在新灾难域预留备席和建立新计划。已有主站不能通过 `intent assign_primary` 直接改写，必须走围栏交接。
