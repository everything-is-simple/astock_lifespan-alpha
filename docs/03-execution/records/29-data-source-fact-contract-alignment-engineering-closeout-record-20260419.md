# 阶段七批次 29 data 源事实契约工程收口记录

记录编号：`29`
日期：`2026-04-19`

## 1. 做了什么

1. 扩展 `SourceFactDatabasePaths`，登记 6 个本地 source fact DuckDB。
2. 对齐 `malf / alpha / position / trade` source adapter，使其支持真实 stock adjusted 表和字段映射。
3. 保留旧表名 fallback。
4. 新增 source contract 单元测试。
5. 新增 `29` 号工程收口治理闭环。
6. 更新 README、docs 索引与结论目录，把当前状态切换为“阶段七完成，阶段八 data -> system 编排待规划”。

## 2. 偏差项

- 本批次未实现 `data -> system` 全线编排。
- 本批次未纳入 index/block。

## 3. 备注

- 阶段八可以在此基础上规划最小 pipeline runner，统一触发已完成的各模块 runner。

