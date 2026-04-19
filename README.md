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

当前已完成阶段三 `alpha` 五账本与 `alpha_signal`，重点是：

- 五个 PAS trigger 正式规格与 `alpha_signal` 汇总规格已冻结
- `alpha_bof / tst / pb / cpb / bpb / alpha_signal` 账本已建立
- 六个 alpha runner、共享输入层、queue、checkpoint 与 replay 骨架已落地
- `alpha_signal` 已形成阶段四 `position` 可读输入面
- 阶段三执行闭环 `08-14` 已补齐

这不代表业务逻辑已经实现完成。

当前阶段更准确的含义是：

> `MALF -> alpha` 正式主线已经成立，下一阶段将进入 `position` 接口切换与最小主线验证

## 文档入口

正式文档请从 [docs/README.md](H:\astock_lifespan-alpha\docs\README.md) 开始阅读。
