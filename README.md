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

## 共享电表碳结算（carbon 扩展）

集群总表读数没有租户字段，不能按租户过滤 `power_readings`；`carbon` 扩展把核定总读数依据 `placement_leases`、`capacity_claims` 与 `workload_intents` 还原到客户账单。旧的 `order/reading/statement` 命令与 `carbon-normal/failure.json` 行为保持不变。

证据与分摊要点：

- `reading`（增强字段 `meter/start/end/surveyor/evidence/revision`）按 `(id, revision)` 保存全部版本；同代次重复上传返回 `duplicate_reading`，旧代次迟到返回 `stale_reading_revision`，同代次改内容返回 `reading_revision_conflict`，更正只追加新版本，不覆盖原读数。
- `agreement` 预签分摊办法：`method=run_hours`（按核定实际运行小时，权重来自 `load_window` 的占用/预热/复制时长）或 `guaranteed_capacity`（按 `capacity_claims.cpu` 保障容量）；`negotiation` 选定本期条款并记录 `standby_bearer`（`standby_owner` 或 `host`）。
- `load_window` 用 `kind=occupied|preheat|replication|reserved` 分开实际占用、热备预热、故障迁移复制与仅预留；预留窗口不计运行小时。跨账期复制按账期边界切分，`reconcile_workload` 核对跨集群负载且同集群窗口不重叠。
- `settle`/`recalculate` 用最大余数法做整数分摊，各客户份额加待决量严格等于可信总读数。缺失占用窗口通过 `evidence_request` 挂 `hold_units/hold_amount` 与补证 `deadline`，绝不按均值造窗口；`expire_evidence` 判定逾期，补证后再重算。
- 账单 `carbon_bills` 独立维护 `issue_state/pay_state/dispute_state` 与 `ceiling_units/excess_units/penalty_amount/total_due`。`issue` 签发后财务字段冻结；新读数只能经 `recalculate` 生成新版本账单并以 `prior_bill` 链接，旧账单（含已付款）原样保留，付款沿版本链结转，多付产生 `refund_due`。`pay`、`dispute`、`resolve_dispute` 只推进各自状态。

十一月 C7 场景见 `fixtures/carbon-shared-meter.json`（封装 `{subsystem,payload}`，可直接 `python -m cloudcourt carbon --db c7.sqlite --fixture fixtures/carbon-shared-meter.json` 后 `replay`）：核定 100 单位，甲生产主席（权重 43200 分钟，含跨月复制 72h）与乙热备（14400 分钟）在小时法下为 75/25；保障容量 4:6 在容量法下为 40/60，两者碳余额、超额赔付与应付均不同。`fixtures/carbon-missing-evidence.json` 覆盖缺证待决与补证重算。验收测试为 `tests/test_carbon_settlement.py`：

```sh
python -B -m unittest discover -s tests -q
```

