# 联邦负载安置与撤离庭

离线多集群控制面模拟，Python 3.11+ 标准库与 SQLite 即可运行。它只处理法域合同、集群席位和故障接管，不连接 Kubernetes 或云 API。

## 三个入口

- `intent`：租户法域规则、负载合同与主站授权。合同冻结保留既有负载，但阻止新席位；授权代次决定谁能指定主站。
- `seat`：集群资源向量、灾难域和主备席位。并发预留在 SQLite IMMEDIATE 事务中结算，旧观测、同灾难域和重复主备角色均阻断。
- `rehearsal`：灾备演练的独立证据庭。开演前封存负载清单、合同版本、主备席代次与容量观测；演练只允许使用获准的影子席位与隔离数据，不修改 `primary_cluster`、不释放生产 claim、不写客户真实序号。
- `evac`：事故等级、撤离计划和双端交接回执。只有源端围栏与目标端就绪的当前集群代次均成立，才转移主站并释放旧席位。

这三个入口分别维护 `workload_intents/residency_rules/owner_grants`、`cluster_inventory/capacity_claims/placement_leases`、`incident_cases/move_plans/handoff_receipts`。每条命令保存原始输入、裁决及业务表，可从事件顺序重放并逐表核对。

## 运行

在仓库根目录：

```sh
python -m cloudcourt demo --db demo.sqlite
python -m cloudcourt replay --db demo.sqlite
python -m unittest discover -s tests -v
```

`fixtures/<入口>-normal.json` 和 `fixtures/<入口>-failure.json` 分别给出正常与改变业务结论的反例；`fixtures/path.json` 在席位就绪后指定原主站，`demo` 再运行事故接管。独立入口命令如 `python -m cloudcourt seat --db state.sqlite --fixture fixtures/seat-normal.json`。首次运行请使用新数据库文件。

关键交互：法域合同限制候选集群；主备席位的灾难域与资源占用改变撤离计划；双端回执缺失或目标换代时，旧主站仍保持不变。测试覆盖并发容量、部分交接及目标重建。

目标换代使旧交接回执失效时，`evac abort` 会保留恢复案、释放旧目标备席，再在新灾难域预留备席和建立新计划。已有主站不能通过 `intent assign_primary` 直接改写，必须走围栏交接。

## 灾备演练与证明等级

`rehearsal` 不复用 `evac handoff`：它只读取生产表做封存与身份核对，所有写入都在 `rehearsal_*` 表。典型顺序是 `offer_shadow → seal → consent → reserve_shadow → start → evidence(source_fenced/target_ready/probe) → certify`。每份原件都绑定同一演练代次 `generation` 与隔离作用域 `scope-<演练>-g<代次>`；目标节点 `rebuild` 换代或客户 `withdraw` 许可后，旧代次/迟到证据只会留下 `valid=0` 的拒收记录，不能补签。失败或超时通过 `fail`/`timeout` 留下原因，并把影子占用全部置为 `released` 完成清偿。

证明等级由原件推导：仅布尔/模拟材料至多得到 `simulated`（结论 `not_takeover_proof`，不得冒充接管证明）；隔离影子席位、隔离围栏、指定恢复点与客户探针时限齐全得到 `shadow_end_to_end`；再有客户确认停写窗口与恢复点逐笔对账才得到 `production_verified`，并记录客户停写损失。超过当天 `deadline` 出证时结论为 `filable_late` 而非 `filable_today`。`fixtures/rehearsal-normal.json` 与 `fixtures/rehearsal-failure.json` 分别演示完整强证明和容量不足/探针超时/清偿。验收测试在同一库中先演练后真实事故，并逐表核对演练前后生产状态不变：

```sh
python -m unittest discover -s tests -v
```
