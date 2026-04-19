# 阶段四批次 21 本地收口执行卡

卡片编号：`21`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段四 `15-20` 完成后，需要把文档、实现、测试与索引统一收口。
- 目标：确认阶段四已经正式结束，并切换顶层状态到阶段五待规划。
- 为什么现在做：没有收口，阶段四仍停留在工作区状态而不是正式治理状态。

## 2. 设计输入

- `docs/03-execution/20-position-materialization-and-portfolio-bridge-conclusion-20260419.md`

## 3. 规格输入

- `docs/02-spec/05-alpha-signal-to-position-bridge-spec-v1-20260419.md`
- `docs/02-spec/06-position-minimal-ledger-and-runner-spec-v1-20260419.md`
- `docs/02-spec/07-portfolio-plan-minimal-bridge-spec-v1-20260419.md`

## 4. 任务切片

1. 更新顶层状态说明。
2. 确认阶段四测试通过。
3. 把阶段四所有批次纳入结论目录与执行区索引。

## 5. 实现边界

范围内：

- 顶层 `README`
- `docs/README.md`
- `docs/03-execution/README.md`
- `docs/03-execution/00-conclusion-catalog-20260419.md`

范围外：

- 阶段五 `trade`

## 6. 收口标准

1. 阶段四被明确标记为已完成。
2. 阶段五 `trade` 被标记为下一阶段。
3. 阶段四所有文档、代码与测试均在索引中可追溯。
