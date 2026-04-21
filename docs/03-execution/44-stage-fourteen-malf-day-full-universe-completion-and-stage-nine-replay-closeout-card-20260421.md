# 批次 44 卡片：阶段十四 MALF day full-universe completion 与阶段九 replay 收口

批次编号：`44`
日期：`2026-04-21`
文档标识：`stage-fourteen-malf-day-full-universe-completion-and-stage-nine-replay-closeout`

## 1. 任务

完成阶段十五之后遗留的 MALF day full-universe promotion 与阶段九真实 replay 收口。

本批次同时处理真实 replay 暴露的 full-universe scalability 问题，并将无进程对应的 `running` run 记录显式治理为 `interrupted`。

## 2. 范围

本批次处理：

- MALF day remaining symbols、full-universe promote 与正式库接受登记
- alpha / position / trade full-universe replay 的 set-based scalability fix
- `trade` / `pipeline` orphan running run 治理
- `trade` 中断后主订单表清空的恢复与事务保护
- 阶段九 `data -> system` pipeline replay 完整收口

本批次不处理：

- MALF 业务语义调整
- guard anchor / reborn window / 历史谱系 profile
- 新增 public runner API、CLI、schema 或业务表结构
- building artifact archive/remove

## 3. 执行命令

核心真实执行顺序：

```text
python scripts/malf/run_malf_day_build.py --start-symbol 688618.SH --symbol-limit 1000
python scripts/malf/run_malf_day_build.py
python scripts/malf/run_malf_week_build.py
python scripts/malf/run_malf_month_build.py
python scripts/alpha/run_alpha_bof_build.py
python scripts/alpha/run_alpha_tst_build.py
python scripts/alpha/run_alpha_pb_build.py
python scripts/alpha/run_alpha_cpb_build.py
python scripts/alpha/run_alpha_bpb_build.py
python scripts/alpha/run_alpha_signal_build.py
python scripts/position/run_position_from_alpha_signal.py
python scripts/portfolio_plan/run_portfolio_plan_build.py
python scripts/trade/run_trade_from_portfolio_plan.py
python scripts/system/run_system_from_trade.py
python scripts/pipeline/run_data_to_system_pipeline.py
pytest
```

## 4. 验收

- `malf_day.duckdb` 正式库接受为当前正式结果
- `malf_day.duckdb` 与 `malf_day.backup-day-fc56ff5e5441.duckdb` 核心业务列双向 `EXCEPT = 0`
- 最新 pipeline run 完整完成 13 步
- 最新 trade / system run 均为 `completed`
- 全量 `pytest` 通过
