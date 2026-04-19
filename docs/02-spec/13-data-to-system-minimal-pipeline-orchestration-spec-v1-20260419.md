# 阶段八 data -> system 最小 pipeline orchestration 规格 v1

日期：`2026-04-19`
状态：`冻结`
文档标识：`stage-eight-pipeline`

## 1. 定位

本规格冻结阶段八 `data -> system` 最小 pipeline orchestration。

阶段八只新增一个薄编排层，把已经完成的正式 runner 串成统一入口，不修改各模块业务语义，不绕过模块边界，不直接写业务表。

阶段八主线固定为：

```text
data -> malf -> alpha -> position -> portfolio_plan -> trade -> system
```

## 2. 正式接口

阶段八新增 runner：

```text
run_data_to_system_pipeline(portfolio_id: str = "core", settings: WorkspaceRoots | None = None) -> PipelineRunSummary
```

稳定契约：

- `PIPELINE_CONTRACT_VERSION = "stage8_pipeline_v1"`
- `PipelineRunStatus.COMPLETED`
- `PipelineRunSummary`
- `PipelineStepSummary`

## 3. pipeline 表族

阶段八新增两张正式表：

- `pipeline_run`
- `pipeline_step_run`

`pipeline_run` 记录整次 pipeline 的 run id、status、portfolio_id、step 数、message、started_at、finished_at。

`pipeline_step_run` 记录每个 step 的顺序、runner_name、runner_run_id、runner_status、target_path、message、summary_json。

pipeline 只记录 step summary，不直接写 `malf / alpha / position / portfolio_plan / trade / system` 业务表。

## 4. 固定 runner 顺序

阶段八 runner 顺序固定为：

1. `run_malf_day_build`
2. `run_malf_week_build`
3. `run_malf_month_build`
4. `run_alpha_bof_build`
5. `run_alpha_tst_build`
6. `run_alpha_pb_build`
7. `run_alpha_cpb_build`
8. `run_alpha_bpb_build`
9. `run_alpha_signal_build`
10. `run_position_from_alpha_signal`
11. `run_portfolio_plan_build`
12. `run_trade_from_portfolio_plan`
13. `run_system_from_trade`

所有 step 必须通过各模块 public runner 调用。

## 5. 行为规则

- pipeline runner 必须使用同一个 `WorkspaceRoots`。
- `portfolio_id` 必须传给 `run_portfolio_plan_build`、`run_trade_from_portfolio_plan`、`run_system_from_trade`。
- empty summary 不视为失败，pipeline 继续执行后续 step。
- runner 抛异常时，本轮不定义恢复策略，由异常直接暴露。
- `system` 仍只读取 `trade` 正式输出。

## 6. 明确不纳入阶段八

阶段八不纳入：

- 新业务语义
- 修改阶段二到阶段七已冻结 runner 规则
- scheduler / 定时任务
- 外部服务
- broker/session/partial fill
- pnl
- exit
- pipeline 直接写业务表

## 7. 验收标准

阶段八工程实施必须满足：

1. `run_data_to_system_pipeline` 可以按固定顺序调用 13 个 public runner。
2. `pipeline_run / pipeline_step_run` 可以记录 pipeline 与 step summary。
3. pipeline 不直接写业务表。
4. 单元测试使用临时 workspace，不读取真实 `H:\Lifespan-data` 大库。
5. docs 测试、pipeline 测试、contracts 测试与全量测试通过。

