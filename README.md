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

当前已完成阶段二 `MALF` 冻结与构建，重点是：

- `MALF` 文本规格与图版规格正式冻结
- `malf_day / malf_week / malf_month` 三周期正式账本已建立
- `MALF` runner、queue、checkpoint 与 rebuild flow 已落地
- `malf_wave_scale_snapshot / malf_wave_scale_profile` 已形成 `alpha` 可读输入面
- 阶段二执行闭环 `02-06` 已补齐

这不代表业务逻辑已经实现完成。

当前阶段更准确的含义是：

> `MALF` 作为正式市场结构真值层已经成立，下一阶段将进入 `alpha` 五账本与 `alpha_signal` 的正式重构

## 文档入口

正式文档请从 [docs/README.md](H:\astock_lifespan-alpha\docs\README.md) 开始阅读。
