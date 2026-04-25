# Card 67 run_id key boundary contract 工程结论

日期：`2026-04-25`
状态：`已接受`

## 结论

Card 67 已冻结正式表主键边界。

当前正式裁决：

- `ledger / checkpoint` 主键不得包含 run lineage 字段。
- `run_audit` 可以用 `run_id` 或 `pipeline_run_id` 参与主键。
- `queue` 是每次运行的执行痕迹，不是业务主账本或断点续传账本。
- 下游不得把 `*_work_queue` 或 run-scoped mapping 表当作唯一事实源。

## 已落地

- 新增 `tests/unit/contracts/test_run_id_key_boundaries.py`。
- 表分类白名单覆盖当前正式 data/malf/alpha/position/portfolio_plan/trade/system/pipeline schema。
- `ledger / checkpoint` 主键禁止 `run_id / runner_run_id / pipeline_run_id / last_run_id / last_pipeline_run_id`。
- `*_work_queue` 保留现有 per-run `queue_id` 口径。

## 验证证据

验证命令与结果登记在：

- `docs/03-execution/evidence/67-run-id-key-boundary-contract-evidence-20260425.md`

最终全量验证结果：`150 passed`。

## 最终裁决

Card 67 接受。

当前主业务账本没有使用 `run_id` 作为主要数据库 key；run lineage 继续留在运行日志、审计历史、checkpoint 归因与物化标记字段中。
