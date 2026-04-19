# 阶段五批次 25 trade 工程收口执行卡

卡片编号：`25`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段五已经完成文档冻结与 `trade` 工程实现，但尚缺一份正式 Part 2 文档把“文档先行与工程实施计划”落档。
- 目标：创建 `reconstruction-plan-part2`，并裁决阶段五完成、阶段六 `system` 待规划/待实施。
- 为什么现在做：避免主重构计划仍停留在“阶段五工程待启动”的旧状态。

## 2. 任务切片

1. 新增重构计划 Part 2。
2. 更新 README、docs 索引和结论目录。
3. 新增 docs 测试锁定 Part 2 关键词。
4. 验证 docs 测试与全量测试。

## 3. 收口标准

1. Part 2 文档明确 `portfolio_plan -> trade` 已完成。
2. 文档明确 `PathConfig.source_databases.market_base`、`execution_price_line`、次日开盘执行、`filled / rejected`、`accepted` 保留和 `portfolio_id + symbol`。
3. 文档明确阶段六 `system` 是下一阶段入口。
