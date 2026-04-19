# astock-lifespan-alpha

`astock-lifespan-alpha` 是从 `lifespan-0.01` 重构出来的新系统仓库。

正式主链路为：

```text
data -> malf -> alpha -> position -> portfolio_plan -> trade -> system
```

当前仓库明确移除了 `structure` 与 `filter` 作为正式系统架构的一部分。

首版实现技术栈冻结为：

- Python
- DuckDB
- Arrow

## 当前阶段

当前已完成阶段一基础重构，重点是：

- 独立仓库与独立环境
- 五根目录工作区契约
- 新账本命名空间 `astock_lifespan_alpha`
- `malf / alpha / position` 的最小 runner stub
- 文档治理闭环恢复

这不代表业务逻辑已经实现完成。

当前阶段更准确的含义是：

> 新系统底盘已经成立，后续 `MALF / alpha / position` 等模块可以在正式治理下继续重构

## 文档入口

正式文档请从 [docs/README.md](H:\astock_lifespan-alpha\docs\README.md) 开始阅读。
