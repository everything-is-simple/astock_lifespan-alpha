# 阶段七批次 29 data 源事实契约工程收口执行卡

卡片编号：`29`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段七规格已冻结且 source fact contract 工程对齐已完成，需要正式治理收口。
- 目标：裁决阶段七 data 源事实契约对齐完成，并把仓库状态切换为“阶段七完成，阶段八 data -> system 编排待规划”。
- 为什么现在做：避免工程实现已落地但治理状态仍停留在“工程待实施”。

## 2. 规格输入

- `docs/02-spec/12-data-source-fact-contract-alignment-spec-v1-20260419.md`
- `docs/03-execution/28-data-source-fact-contract-alignment-spec-freeze-conclusion-20260419.md`

## 3. 任务切片

1. 新增批次 `29` 的 card/evidence/record/conclusion。
2. 更新 README、docs 索引与结论目录。
3. 新增 docs 测试锁定阶段七完成结论。
4. 验证 docs 测试与全量测试。

## 4. 收口标准

1. 6 个 source fact 路径已进入 `SourceFactDatabasePaths`。
2. `malf` 支持 day/week/month 真实 stock adjusted 表。
3. `alpha / position / trade` 支持 `stock_daily_adjusted`。
4. `code -> symbol` 与 `trade_date` 映射已被测试覆盖。
5. 阶段七完成结论已登记，阶段八 `data -> system` 编排待规划。

