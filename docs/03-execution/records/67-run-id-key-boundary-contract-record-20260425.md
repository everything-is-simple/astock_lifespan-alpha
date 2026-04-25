# Card 67 run_id key boundary contract 记录

日期：`2026-04-25`

## 做了什么

1. 新增 `run_id` key boundary 契约测试。
2. 将正式 schema 表分为 `ledger / checkpoint / queue / run_audit` 四类。
3. 明确 `*_work_queue` 是运行痕迹，断点续传继续由 checkpoint 与 source fingerprint 口径承担。
4. 更新执行结论目录。

## 偏差项

- 本卡不改现有 `queue_id = run_id + work_unit` 生成规则；该规则被正式解释为 run trace，不再解释为稳定 checkpoint key。
- `raw_ingest_file` 仍无物理主键；本卡将其归入 `run_audit`，不把它定义为业务主账本。

## 备注

- 当前业务主账本仍以 `*_nk` 或自然组合键作为主键。
- `run_id`、`first_seen_run_id`、`last_materialized_run_id`、`last_run_id` 继续作为运行谱系与物化标记字段使用。
