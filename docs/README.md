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
12. `docs/03-execution/README.md`
13. `docs/03-execution/20-position-materialization-and-portfolio-bridge-conclusion-20260419.md`
14. `docs/03-execution/21-stage-four-closeout-conclusion-20260419.md`

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

当前仓库已经完成阶段四 `position` 接口切换与 `portfolio_plan` 最小桥接。

这意味着：

- 阶段一到阶段四的实际交付范围都已经进入正式治理闭环
- `position` 已具备正式规格、独立账本、queue/checkpoint、正式 runner 与三层最小输出
- `portfolio_plan` 已具备最小三表、最小组合裁决与正式 runner
- 阶段五 `trade` 尚未开始实施，后续仍必须沿用同样的执行纪律
