# 阶段九批次 32 真实建库演练规格冻结结论

结论编号：`32`
日期：`2026-04-19`
状态：`已接受`

## 1. 裁决

- 接受：阶段九 `stage-nine-real-data-build` 规格已冻结。
- 接受：真实建库演练固定采用 `module-by-module build` 后接 `pipeline replay`。
- 拒绝：在本批次把阶段九解释为 Go+DuckDB 迁移、业务语义扩展或 source/schema 热修复。

## 2. 原因

- 阶段八已经完成统一 pipeline 入口，但尚未在真实 `H:\Lifespan-data` 上做正式演练。
- 正式输出目录 `H:\Lifespan-data\astock_lifespan_alpha` 已存在，真实建库需要先冻结写入边界与 preflight 记录要求。
- 真实建库演练应优先验证现有 Python+DuckDB 主线，而不是提前新开 Go+DuckDB 分支。

## 3. 影响

- 阶段九从本批次之后才允许进入真实建库执行。
- 执行前必须先做 read-only preflight。
- 当前仓库状态切换为：阶段九规格冻结，真实建库待执行。
