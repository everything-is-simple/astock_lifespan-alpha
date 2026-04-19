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
5. `docs/03-execution/README.md`
6. `docs/03-execution/01-foundation-bootstrap-card-20260419.md`
7. `docs/03-execution/01-foundation-bootstrap-conclusion-20260419.md`

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

当前仓库已经补回阶段一基础重构的治理闭环。

这意味着：

- 第一阶段实际交付范围已经被正式记录
- 后续阶段不再默认允许“先改代码，后补文档”
- 后续 `MALF / alpha / position` 重构应继续沿用同样的执行纪律
