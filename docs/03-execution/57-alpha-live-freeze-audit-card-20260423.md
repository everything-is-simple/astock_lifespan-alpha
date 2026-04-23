# 批次 57 卡片：alpha live freeze audit

卡片编号：`57`
日期：`2026-04-23`
文档标识：`alpha-live-freeze-audit`

## 目标

在 Card 56 已确认 `pipeline = 放行` 的前提下，只处理当前 `astock` 正式 `alpha` producer 合同的 live freeze audit。

本轮目标固定为：

- 验收当前 `alpha` 能稳定消费正式 `market_base + malf_day`。
- 验收 5 个 trigger 与 `alpha_signal` 的正式账本在 live formal DuckDB 中口径一致。
- 验收 `position` 当前继续消费正式 `alpha_signal`，且不需要反向修复。
- 正式登记当前 `astock alpha` 与历史 PAS/alpha 体系的差距清单。
- 不升级 `alpha` 合同。
- 不进入 `malf`。
- 不进入 `data`。

## 验收口径

- 最新 5 个 `alpha_run.status = completed`
- 最新 `alpha_signal_run.status = completed`
- 5 个 trigger 库的 `alpha_checkpoint` 都覆盖 `5501` 个 symbol
- `alpha_signal_checkpoint` 覆盖 `5` 个 source trigger db
- `alpha_signal` 仍为阶段三唯一正式输出账本
- `alpha_signal` 继续只保留：
  - `signal_nk`
  - `trigger_type`
  - `formal_signal_status`
  - `source_trigger_db`
  - MALF 波段位置字段
- `position_run.alpha_source_path` 指向正式 `H:\Lifespan-data\astock_lifespan_alpha\alpha\alpha_signal.duckdb`
- `position_candidate_audit` 行数与正式 `alpha_signal` 行数保持一致
- Card 57 evidence 必须包含 legacy delta table

## 本轮边界

- 只审计 `src/astock_lifespan_alpha/alpha/` 当前正式合同与 live formal DB。
- 历史仓 `G:\history-lifespan\...` 与 `H:\Lifespan-Validated` 只作为审计参照，不作为当前实现真相源。
- 不修改 `alpha` 代码。
- 不修改 `position`。
- 不新增 PAS 因子评分、机会等级、风险收益比或 16-cell/readout 体系。
- 若只发现 stale `running` 账本，最多只在 `alpha` ledger 内登记/修正为 `interrupted`；若发现语义缺口，只记录 blocker，不在本卡实现升级。
