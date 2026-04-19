# 阶段二批次 07 本地收口执行卡

卡片编号：`07`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段二 `02-06` 的 MALF 文档、实现和测试已经完成，但当前仍停留在未提交的工作区状态。
- 目标：把阶段二从“工作区已完成”收口为“仓库本地已正式落地”。
- 为什么现在做：如果不补本地收口，仓库对外状态、执行闭环和 git 主线会继续脱节，阶段三无法作为干净起点启动。

## 2. 设计输入

- `docs/03-execution/00-card-execution-discipline-20260419.md`
- `docs/03-execution/05-malf-engine-and-runner-conclusion-20260419.md`
- `docs/03-execution/06-malf-alpha-facing-outputs-conclusion-20260419.md`

## 3. 规格输入

- `docs/02-spec/00-astock-lifespan-alpha-reconstruction-master-plan-v1-20260419.md`
- `docs/02-spec/01-malf-wave-scale-semantic-spec-v1-placeholder-20260419.md`
- `docs/02-spec/02-malf-wave-scale-diagram-edition-placeholder-20260419.md`

## 4. 任务切片

1. 清点阶段二 `02-06` 的改动面，确认没有遗漏在执行闭环之外的孤立文件。
2. 更新根 `README` 和 `docs/README`，把仓库对外状态对齐到“阶段二已完成，阶段三待启动”。
3. 新增 `07` 的 `card / evidence / record / conclusion`，记录阶段二本地收口过程。
4. 跑最终测试与 git 核对，并完成一个本地提交。

## 5. 实现边界

范围内：

- 阶段二 `02-06` 的 MALF 文档、代码、测试
- `README.md`
- `docs/README.md`
- `docs/03-execution/`
- 本地 git 提交

范围外：

- `alpha` 五账本实现
- `position` 上游切换
- 远端 push、PR 或发布流程

## 6. 收口标准

1. 阶段二全部相关文件被纳入单一本地提交。
2. `README`、`docs/README`、`03-execution` 索引与结论目录对阶段状态描述一致。
3. `pytest` 全通过。
4. 提交后工作区为干净状态。
