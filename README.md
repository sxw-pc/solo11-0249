# 联邦负载安置与撤离庭

离线多集群控制面模拟，Python 3.11+ 标准库与 SQLite 即可运行。它只处理法域合同、集群席位和故障接管，不连接 Kubernetes 或云 API。

## 四个入口

- `intent`：租户法域规则、负载合同与主站授权。合同冻结保留既有负载，但阻止新席位；授权代次决定谁能指定主站。
- `seat`：集群资源向量、灾难域和主备席位。并发预留在 SQLite IMMEDIATE 事务中结算，旧观测、同灾难域和重复主备角色均阻断。
- `evac`：事故等级、撤离计划和双端交接回执。只有源端围栏与目标端就绪的当前集群代次均成立，才转移主站并释放旧席位。
- `hardware`：部件批次、序列号监管链与维修单。召回通知独立登记受影响批次与失效模式，不改写批次状态或历史安装事实；拆卸须独立见证人确认客户密钥已清除或封存；出库、承运、维修中心收件、返还四段交接各有时间与完整性证据，缺一段即不得认定供应商收到故障件；替换件须通过供应商签章、型号兼容、实际序列号与质量证明核验并完成物理验收，否则 `seat` 拒绝该集群的健康观测；维修成本、保修豁免与客户停机损失分项结算，供应商迟到回执不倒填服务恢复时刻。

这四个入口分别维护 `workload_intents/residency_rules/owner_grants`、`cluster_inventory/capacity_claims/placement_leases`、`incident_cases/move_plans/handoff_receipts`、`hardware_lots/hardware_units/repair_orders/recall_notices/vendor_attestations/custody_events/transport_legs/replacement_acceptances/vendor_acks/settlements`。每条命令保存原始输入、裁决及业务表，可从事件顺序重放并逐表核对。

## 运行

在仓库根目录：

```sh
python -m cloudcourt demo --db demo.sqlite
python -m cloudcourt replay --db demo.sqlite
python -m unittest discover -s tests -v
```

`fixtures/<入口>-normal.json` 和 `fixtures/<入口>-failure.json` 分别给出正常与改变业务结论的反例；`fixtures/path.json` 在席位就绪后指定原主站，`demo` 再运行事故接管。`fixtures/hardware-recall.json` 给出 C7 加密卡召回批次全流程：签章入库、召回发布、见证拆卸、四段交接、备用卡验收与现场恢复、迟到的供应商回执及分项结算，由 `tests/test_hardware_recall.py` 驱动。独立入口命令如 `python -m cloudcourt seat --db state.sqlite --fixture fixtures/seat-normal.json`。首次运行请使用新数据库文件。

关键交互：法域合同限制候选集群；主备席位的灾难域与资源占用改变撤离计划；双端回执缺失或目标换代时，旧主站仍保持不变。测试覆盖并发容量、部分交接及目标重建。

目标换代使旧交接回执失效时，`evac abort` 会保留恢复案、释放旧目标备席，再在新灾难域预留备席和建立新计划。已有主站不能通过 `intent assign_primary` 直接改写，必须走围栏交接。
