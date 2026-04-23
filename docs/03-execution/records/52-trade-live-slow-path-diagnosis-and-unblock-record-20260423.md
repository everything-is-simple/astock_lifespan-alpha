# 批次 52 记录：trade live slow-path diagnosis and unblock

记录编号：`52`
日期：`2026-04-23`
文档标识：`trade-live-slow-path-diagnosis-and-unblock`

## 1. 执行顺序

1. 在 `trade` runner 中补齐 phase recorder，并把进度同步写入 `trade_run.message` 与 stderr。
2. 新增只读脚本 `scripts/trade/profile_trade_live_path.py`，复用正式 source/view 但不写正式表。
3. 将 work-unit 指纹改成固定宽度哈希聚合，消除旧 `string_agg` OOM。
4. 将 `intent / execution` 物化拆成阻塞行直落与 actionable 行价格联接两路。
5. 为 materialized action tables 补 `compare_signature`，把 classification 改成窄比较。
6. 运行代码侧 gate：
   - `pytest tests/unit/trade -q`
   - `pytest tests/unit/contracts/test_module_boundaries.py -q`
   - `pytest tests/unit/docs/test_trade_specs.py -q`
7. 跑只读 profile，确认 slow path 已能完整返回，主瓶颈落在 `intent_materialized / action_tables_ready`。
8. 第一次正式 live rerun：
   - 推进到 `write_transaction_started`
   - 观察两个窗口无进展
   - 标记 `trade-5b93a1f466f8 = interrupted`
9. 将 delete 路径改为 `(portfolio_id, symbol)` 联接删写后，第二次正式 live rerun：
   - 仍停在 `write_transaction_started`
   - 标记 `trade-b68fe7bc930e = interrupted`
10. 在写事务内补 `write_targets_cleared / write_output_tables_loaded / write_tracking_tables_loaded` 子阶段后，第三次正式 live rerun：
    - 仍未出现 `write_targets_cleared`
    - 标记 `trade-dbb7397cbd43 = interrupted`
11. 回查正式库，确认 `checkpoint / work_queue / 正式结果表` 仍未从旧态推进。

## 2. 偏差项

- 本轮没有达到 Card 51 的正式 pass 线，`trade` 仍不能登记为 `放行`。
- delete rewrite 并未消除正式写事务长挂，因此当前修复还没有走到正式落库成功。
- 本轮新增的写事务子阶段表明，卡点仍早于 `write_targets_cleared`，后续必须继续拆 delete 路径，而不是回头重做前半段 slow path。

## 3. 备注

- 本轮没有进入 `system`。
- 本轮没有改 public runner 名称。
- 本轮没有 bump `trade_contract_version`。
- Card 51 保持原结论不回写；Card 52 作为新的 active card 收口。
