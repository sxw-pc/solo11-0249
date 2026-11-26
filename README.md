# 联邦负载安置与撤离庭

离线多集群控制面模拟，Python 3.11+ 标准库与 SQLite 即可运行。它只处理法域合同、集群席位和故障接管，不连接 Kubernetes 或云 API。

## 三个入口

- `intent`：租户法域规则、负载合同与主站授权。合同冻结保留既有负载，但阻止新席位；授权代次决定谁能指定主站。
- `seat`：集群资源向量、灾难域和主备席位。并发预留在 SQLite IMMEDIATE 事务中结算，旧观测、同灾难域和重复主备角色均阻断。
- `evac`：事故等级、撤离计划和双端交接回执。只有源端围栏与目标端就绪的当前集群代次均成立，才转移主站并释放旧席位。

这三个入口分别维护 `workload_intents/residency_rules/owner_grants`、`cluster_inventory/capacity_claims/placement_leases`、`incident_cases/move_plans/handoff_receipts`。每条命令保存原始输入、裁决及业务表，可从事件顺序重放并逐表核对。

## custody：可追责的密钥释放与恢复

`custody` 在最初的 `key/vote/release` 上扩展。未建立治理名册的旧密钥仍按“两票”兼容裁决（但同一 actor 的重复投票只计一次）；一旦该租户有保管人名册、客户许可或释放案，`release` 必须逐项通过并把依据快照写入 `release_audits`：

1. **归属**：`key_epochs.tenant/intent` 与 `workload_intents` 一致且合同未撤销；
2. **席位**：`placement_leases` 中目标集群有当前代次的 `occupied` 席位、库存健康且代次一致；只读限时包可交真实占席的备席，`read_write` 还要求 `workload_intents.primary_cluster` 已是目标（主席位）；
3. **事故与客户本次许可**：`recovery_cancellations` 未取消，`customer_consents` 有效、未撤销/过期且 `modes` 覆盖本次 `read`/`read_write` 用途；
4. **两名独立合格保管人**：`custodians.canonical` 归一自然人身份，`custodian_aliases` 吸收改名/旧名，重复行与同一 canonical 的多张票只算一人，已撤销资格（`state=revoked/revoked_at`）的旧票一律剔除；
5. **密钥代次**：`rotate` 把旧代置为 `rotated` 并建立新代；轮换先于释放提交时，旧代 release 判 `key_epoch_superseded`，旧票不自动平移到新代；
6. **设备即时证明**：`target_devices` 钉住当前代次与黄金测量值，`device_attestations` 必须代次/测量值/nonce 相符、在新鲜度窗口内、业务时间不倒签，且事件顺序必须晚于 `release_case` challenge；设备重建（更大 `generation`）使释放案 pin 与旧证明同时失效；
7. **客户可读边界**：`readable_prefixes`（客户可读前缀）、`tail_writes`（未确认尾部及逐笔对账）、`reencryption_jobs/items`（逐序号重加密进度）分表保存，由 `read_boundary()` 归并出 `readable_seq/write_ready`。占了主席位（`seat_boot_only`）不代表历史数据可读；尾部未清或重加密未完成时 `read_write` 分别以 `tail_not_reconciled`/`reencryption_incomplete` 拒绝。

释放成功只返回 `sealed-package://<release>` 引用与限时 `expires_at`，系统不接收/存储明文 DEK：`run()` 在落事件日志前递归抹除 `plaintext_key/raw_key/key_bytes/...` 字段。fixture、事件、审计导出中均无明文密钥字节。

审计者可用 `custody.audit_release(db, event_id)`：它把该释放事件之前的全部事件重放到内存库并重新裁决，逐字段比对 `release_audits.basis`；`custody.export_audit(db)` 汇总全部释放的复算结果。`store.replay(path)` 仍逐表核对整个事件日志。

客户 W/C0/C1 的两条合法恢复路径由 envelope fixture 给出（命令格式为 `[{"subsystem","payload"}]`，测试自行分发）：

- `fixtures/key-recovery-read.json`：甲撤销+旧票剔除，乙与新增合格保管人丙（改名别名）会签；先释放只读、限时 60 的旧代密钥包，可读序号 8；尾部 9/10 逐笔对账后第二次释放开放读写，序号到 10。含保管人撤销。
- `fixtures/key-recovery-rotate.json`：同一事故 `inc-w` 与同一许可 `consent-w`（`modes=[read,read_write,rotate]`、`dual_path_authorization`）下选择另一条路径：轮换到 epoch 2，旧代 release 失效；6/10 部分重加密阻断 RW，完成 10/10 并立新代前缀后一次开放读写。含轮换。
- `fixtures/key-recovery-races.json`：撤票先于释放 → `custodian_quorum_not_met`；许可撤销先于释放 → `consent_revoked`。测试另覆盖“释放先提交则后来撤回/撤销不改写既成记录”的顺序竞态。

验收测试在 `tests/test_key_recovery.py`，从席位、保管人、设备、密文代次到客户可读序号逐项 subTest/断言，并覆盖重复身份、代次换代、撤回竞态、迟到/补签证明、部分重加密与事故取消。

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
