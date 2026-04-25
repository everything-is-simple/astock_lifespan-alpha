# Card 67 run_id key boundary contract 执行卡

日期：`2026-04-25`
状态：`执行中`

## 目标

冻结正式表的主键边界，防止 `run_id` 被重新引入业务主账本或稳定 checkpoint 主键。

## 范围

本卡纳入：

- 将正式表分为 `ledger / checkpoint / queue / run_audit` 四类。
- 新增契约测试，强制 `ledger / checkpoint` 主键不得包含 `run_id`、`pipeline_run_id`、`runner_run_id`、`last_run_id`、`last_pipeline_run_id`。
- 明确 `*_work_queue` 是每次运行的执行痕迹，不作为断点续传账本或下游唯一事实源。

本卡不纳入：

- 修改 runner 行为。
- 重写现有 `queue_id` 生成规则。
- 为历史 source DB 追加 DuckDB 物理主键约束。

## 表分类裁决

- `ledger`：业务事实与正式物化账本。
- `checkpoint`：稳定断点与源进度状态。
- `queue`：per-run work queue 执行痕迹。
- `run_audit`：run 记录、step 记录、run-scoped mapping 与 action history。

## 验收命令

最少执行：

```powershell
pytest tests/unit/contracts/test_run_id_key_boundaries.py -q
pytest tests/unit/contracts/test_module_boundaries.py tests/unit/contracts/test_runner_contracts.py -q
pytest tests/unit/docs tests/unit/contracts -q
pytest
```

## 裁决规则

全部命令通过，且未修改业务 runner 行为，才能登记为工程收口。
