# 批次 58 卡片：MALF day 纯语义重验 gate

- 卡片编号：`58`
- 日期：`2026-04-23`
- 文档 ID：`malf-day-semantic-revalidation-gate`

## 1. 目标

对 live `malf_day.duckdb` 做一次只读、可复现、正式留痕的 `day` 周期 MALF 纯语义重验。

本卡不改 `engine.py`、`schema.py`、`runner.py`，只建立正式 gate 与证据闭环。

## 2. 验收口径

以 `docs/02-spec/23-stage-eighteen-malf-day-semantic-revalidation-spec-v1-20260423.md` 为本轮正式规格。

验收同时要求：

- 硬规则自动检查
- 固定 12 段样本可视化
- JSON / Markdown summary
- 4 张标准导出表
- `card / evidence / record / conclusion` 正式留痕

## 3. 本轮边界

本轮只验证 MALF 纯语义核，不进入：

- `execution_interface`
- `structure / filter`
- 下游消费层
- `week / month`
- `runner / queue / checkpoint` 工程修复

## 4. 交付物

- 审计实现：
  - `src/astock_lifespan_alpha/malf/audit.py`
  - `scripts/malf/audit_malf_day_semantics.py`
- 测试：
  - `tests/unit/malf/test_audit.py`
- 文档：
  - 本卡
  - evidence
  - record
  - conclusion
