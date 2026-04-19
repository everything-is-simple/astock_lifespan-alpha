# 阶段八批次 30 data -> system pipeline 编排规格冻结结论

结论编号：`30`
日期：`2026-04-19`
状态：`已接受`

## 1. 裁决

- 接受：阶段八 `data -> system` 最小 pipeline orchestration 规格已冻结。
- 接受：pipeline 只调用 public runner 并记录 step summary。
- 拒绝：pipeline 直接写业务表或修改各模块已冻结业务语义。

## 2. 原因

- 阶段七已经完成真实 stock source fact 契约对齐。
- 当前各模块 runner 已经形成可串联的最小正式主线。
- 全线打通应由独立 pipeline 层承担，而不是反向扩大 `system` 的职责。

## 3. 影响

- 阶段八从本批次之后才允许进入 pipeline 工程实现。
- 工程实现必须新增 `pipeline_run / pipeline_step_run`。
- 工程实现必须保持固定 runner 顺序，并使用临时 workspace 测试隔离真实本地大库。

