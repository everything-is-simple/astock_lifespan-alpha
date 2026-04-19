# 阶段九批次 33 真实建库演练执行收口卡

卡片编号：`33`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段九 `stage-nine-real-data-build` 规格已冻结，需要按真实 `H:\Lifespan-data` 执行首轮建库演练并记录结果。
- 目标：基于真实 preflight、模块复跑结果与 pipeline replay 可行性，裁决阶段九是“完成”还是“发现阻塞”。
- 为什么现在做：阶段九的价值不在再写功能，而在尽快暴露真实库、真实路径、真实运行时间下的首个系统瓶颈。

## 2. 规格输入

- `docs/02-spec/14-real-data-build-rehearsal-spec-v1-20260419.md`
- `docs/03-execution/32-real-data-build-rehearsal-spec-freeze-conclusion-20260419.md`

## 3. 执行摘要

1. 已完成 6 个 source fact DuckDB 与正式输出目录的 read-only preflight。
2. 已按真实脚本入口启动 `run_malf_day_build`。
3. 已识别脚本直跑需要显式补 `PYTHONPATH=H:\\astock_lifespan-alpha\\src`。
4. 已记录 `run_malf_day_build` 在真实库上超过 12 分钟未返回的阻塞现象。
5. 已停止首轮演练，转为 blocker 收口。

## 4. 收口标准

1. preflight 结果已记录。
2. 真实执行结果已记录，而不是只保留推测。
3. blocker 位置、现象与现场痕迹已可追溯。
4. README、docs 索引与结论目录已切换到 blocker 状态。
