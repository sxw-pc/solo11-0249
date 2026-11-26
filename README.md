# 联邦负载安置与撤离庭

离线多集群控制面模拟，Python 3.11+ 标准库与 SQLite 即可运行。它只处理法域合同、集群席位和故障接管，不连接 Kubernetes 或云 API。

## 三个入口

- `intent`：租户法域规则、负载合同与主站授权。合同冻结保留既有负载，但阻止新席位；授权代次决定谁能指定主站。
- `seat`：集群资源向量、灾难域和主备席位。并发预留在 SQLite IMMEDIATE 事务中结算，旧观测、同灾难域和重复主备角色均阻断。
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

## 事故理赔与保险清算

`insurance` 在原有 `policy/claim/assess`（暂估，保持向后兼容与可重放）之外，提供面向多租户事故的证据核损与清算状态机：

`policy`（完整保单条款）→ `outage`（客户确认中断）/ `invoice`（费用发票原件）/ `offset`（第三方赔偿、服务抵扣）→ `claim`（绑定负载、席位、中断、发票）→ `adjust`（核损）→ `confirm`（客户确认）→ `settlement`（现金 `cash` 或代付 `direct_pay`）→ `payment_receipt`（实付回执）；其后可用 `dishonour`（退票）、`correction`（迟到修正/补赔）、`subrogate` + `recovery_receipt`（追偿）。

核损会逐单核对 `incident_cases` 事故、`move_plans/handoff_receipts` 接管结果，并用 `placement_leases` 加成功的 `occupy` 事件还原该负载当时在受影响集群的实际占席（源席在交接释放后仍可举证）。未在受影响集群实际占席的租户即使引用同一 `incident` 也得到 `not_affected_by_incident`。发票引用唯一且 `claim_invoices.invoice` 唯一，同一费用不能跨两张理赔单重复提交。

多张有效索赔共享同一保单的单次事故限额与 `policy_year` 年度累计余额；单次免赔在事故内只吸收一次。第三方赔偿与服务抵扣即使只登记在租户+事故名下、未关联索赔，核损时也会自动冲减。暂估、已核损、客户确认、已实付、追偿中/已追偿是不同状态；付款回执与原订单金额只追加不改写，退票追加 `payment_dishonours` 关联记录并重开应付差额，迟到修正只生成 `kind=supplement` 的关联差额订单（补赔仍需确认与回执）。

双租户事故 envelope fixture `fixtures/incident-dual-tenant.json` 搭建甲、乙在 `cluster-a` 的主席、双端交接、保单、中断、发票、抵扣、两单核损、现金/代付与回执、追偿，并含虚假受损租户丙的反例；可直接：

```sh
python -m cloudcourt insurance --db /tmp/dual.sqlite --fixture fixtures/incident-dual-tenant.json
python -m cloudcourt replay    --db /tmp/dual.sqlite
python -B -m unittest discover -s tests -q
```

`tests/test_incident_indemnity.py` 逐单核对事故、实际席位、客户中断、发票、共同限额、免赔、代付与实付回执，并覆盖虚假受损租户、重复费用、跨年度余额、第三方追偿、退票和补赔；每个关键场景都校验事件与逐表重放。
