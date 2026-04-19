# 文档入口

`docs/` 是 `astock-lifespan-alpha` 的正式治理入口。

从 `2026-04-19` 起，当前仓库采用中文文档治理，并固定按下面的顺序组织：

```text
01-design -> 02-spec -> 03-execution
```

执行层继续展开为：

```text
card -> evidence -> record -> conclusion
```

## 推荐阅读顺序

如果要快速进入当前项目，建议按下面顺序读：

1. `docs/01-design/01-doc-governance-charter-20260419.md`
2. `docs/01-design/00-foundation-bootstrap-design-notes-20260419.md`
3. `docs/02-spec/01-doc-governance-and-execution-spec-20260419.md`
4. `docs/02-spec/00-astock-lifespan-alpha-reconstruction-master-plan-v1-20260419.md`
5. `docs/02-spec/01-malf-wave-scale-semantic-spec-v1-placeholder-20260419.md`
6. `docs/02-spec/02-malf-wave-scale-diagram-edition-placeholder-20260419.md`
7. `docs/02-spec/03-alpha-pas-trigger-semantic-spec-v1-20260419.md`
8. `docs/02-spec/04-alpha-signal-aggregation-spec-v1-20260419.md`
9. `docs/02-spec/05-alpha-signal-to-position-bridge-spec-v1-20260419.md`
10. `docs/02-spec/06-position-minimal-ledger-and-runner-spec-v1-20260419.md`
11. `docs/02-spec/07-portfolio-plan-minimal-bridge-spec-v1-20260419.md`
12. `docs/02-spec/08-trade-minimal-execution-ledger-and-runner-spec-v1-20260419.md`
13. `docs/02-spec/09-portfolio-plan-to-trade-bridge-spec-v1-20260419.md`
14. `docs/02-spec/10-astock-lifespan-alpha-reconstruction-plan-part2-stage-five-trade-v1-20260419.md`
15. `docs/02-spec/11-system-minimal-readout-and-runner-spec-v1-20260419.md`
16. `docs/02-spec/12-data-source-fact-contract-alignment-spec-v1-20260419.md`
17. `docs/03-execution/README.md`
18. `docs/03-execution/28-data-source-fact-contract-alignment-spec-freeze-conclusion-20260419.md`
19. `docs/03-execution/29-data-source-fact-contract-alignment-engineering-closeout-conclusion-20260419.md`

## 目录职责

### `docs/01-design/`

回答：

- 为什么这样设计
- 当前生效的治理原则是什么
- 当前阶段的边界和非目标是什么

### `docs/02-spec/`

回答：

- 正式系统边界是什么
- 重构总方案是什么
- 文档治理和执行准入规则是什么

### `docs/03-execution/`

回答：

- 这次批次做了什么
- 用什么命令和测试验证
- 执行过程中有哪些偏差
- 最终裁决是什么

执行层的正式闭环是：

`card -> evidence -> record -> conclusion`

其中：

- `evidence` 只允许放在 `docs/03-execution/evidence/`
- `record` 只允许放在 `docs/03-execution/records/`

## 当前状态

当前仓库已经完成阶段七 data 源事实契约对齐，阶段八 `data -> system` 最小全链路编排待规划。

这意味着：

- 阶段一到阶段四的实际交付范围都已经进入正式治理闭环
- `position` 已具备正式规格、独立账本、queue/checkpoint、正式 runner 与三层最小输出
- `portfolio_plan` 已具备最小三表、最小组合裁决与正式 runner
- `trade` 已具备最小执行账本、桥接规格、正式 runner、CLI 入口与测试闭环
- 阶段六规格冻结闭环 `26` 已补齐
- `stage-six-system` 已冻结 `trade -> system` 最小读出边界
- 阶段六工程收口闭环 `27` 已补齐
- `run_system_from_trade` 与 `system_run / system_trade_readout / system_portfolio_trade_summary` 已落地
- 阶段七规格冻结闭环 `28` 已补齐
- `stage-seven-data-source-contract` 已冻结真实本地 stock source fact 路径、表名与字段映射
- 阶段七工程收口闭环 `29` 已补齐
- `malf / alpha / position / trade` source adapter 已支持真实 stock adjusted 表
- 阶段五之后正式冻结价格分线：
  - `malf / alpha` 属于 `analysis_price_line`
  - `portfolio_plan / trade / system` 属于 `execution_price_line`
- 阶段四 `reference_trade_date / reference_price` 只是最小桥接参考，不等于正式执行价格口径

Stage-five engineering defaults are frozen:
- `execution_price_line` reads `PathConfig.source_databases.market_base` through the `trade` adapter.
- `planned_trade_date / execution_trade_date / execution_price` use 次日开盘执行.
- `accepted` is reserved; the first implementation materializes only `filled / rejected`.

重构计划 Part 2：
- 文档标识：`reconstruction-plan-part2`
- 主题：第五阶段文档先行与工程实施计划
- 裁决：阶段五完成，阶段六 system 待规划/待实施

阶段六 system 规格：
- 文档标识：`stage-six-system`
- 主题：`trade -> system` 最小读出与 runner
- 裁决：阶段六完成，下一阶段待规划
- 边界：只读取 `trade` 正式输出，不回读 `alpha / position / portfolio_plan`，不触发上游 runner

阶段七 data 源事实契约：
- 文档标识：`stage-seven-data-source-contract`
- 主题：6 个本地 source fact DuckDB、stock 表名和 `code -> symbol` / `trade_date -> bar_dt` 字段映射
- 裁决：阶段七完成，阶段八待规划
- 下一阶段入口：阶段八 `data -> system` 最小全链路编排
