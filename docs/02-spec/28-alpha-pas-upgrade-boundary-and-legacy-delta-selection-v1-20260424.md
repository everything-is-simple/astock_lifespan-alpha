# Alpha(PAS) 核心升级边界与 legacy delta selection 规格 v1

状态：`冻结`
日期：`2026-04-24`
对应执行卡：`docs/03-execution/64-alpha-pas-upgrade-boundary-and-legacy-delta-selection-card-20260424.md`

## 1. 文档定位

本文档不是重新冻结 `alpha` 是否可运行，也不是引入新的 PAS scoring engine。

本文档只回答四个问题：

1. 当前 `alpha` 在正式合同中的角色是什么。
2. 历史 PAS 能力中，哪些继续留在正式核心外。
3. 哪些历史经验可以成为下一轮最小治理升级入口。
4. 下一张真正的实现卡应该做什么，不应该做什么。

## 2. 当前正式口径

Card 57 后，当前 `alpha` 已作为正式 producer 放行。

当前正式结论固定为：

- `alpha` 当前是 `trigger ledger producer`
- `alpha` 不是完整 `PAS scoring engine`
- `alpha_signal` 仍是阶段三唯一正式输出账本

当前正式输入仍只允许：

- `market_base_day`
- `malf_day.malf_wave_scale_snapshot`

当前正式输出仍固定为：

- `alpha_trigger_event`
- `alpha_trigger_profile`
- `alpha_signal`

正式短语：

> alpha(PAS) 只消费 MALF 正式字段。

## 3. legacy delta selection

### 3.1 保留在正式核心外

以下历史 PAS 能力继续保留在正式核心外，不进入当前正式合同：

- `opportunity_score`
- `grade`
- `risk_reward_ratio`
- `quality_flag`
- `neutrality`
- `readout`
- `condition matrix`

关于 `16-cell`，当前口径单独冻结为：

- `16-cell` 是前代历史研究素材
- `16-cell` 当前系统不存在
- `16-cell` 不作为下一轮治理候选

这些能力可以继续保留为：

- 历史资料
- audit/readout 背景
- 后续研究卡输入

但不得：

- 进入当前 `alpha_trigger_event`
- 进入当前 `alpha_trigger_profile`
- 进入当前 `alpha_signal`
- 作为 `position` 的正式输入

### 3.2 可进入下一轮治理候选

下一轮只考虑最小治理升级，不考虑 scoring 升级。

允许进入下一轮治理候选的只有：

- trigger trace 命名治理
- registry 命名与读出治理
- 最小 audit/readout 对齐
- runner / script / spec 名称对齐
- `alpha_signal` 与五个 trigger 的追溯面文档契约补齐

这些候选项的共同约束是：

- 不改变正式输入边界
- 不改变正式输出字段
- 不改变当前 schema
- 不引入新的 producer
- 不引入交易或收益反馈

### 3.3 禁止反向污染核心

以下能力明确禁止反向进入当前 `alpha(PAS)` 核心：

- trade 结果
- 收益评价
- 质量过滤
- 退出策略

正式短语：

> 历史 PAS 研究结果不能反向定义当前 alpha(PAS) 合同。

## 4. 当前系统事实

当前系统中，`alpha` 的正式工程面只包括：

- 6 个 public runner
  - `run_alpha_bof_build`
  - `run_alpha_tst_build`
  - `run_alpha_pb_build`
  - `run_alpha_cpb_build`
  - `run_alpha_bpb_build`
  - `run_alpha_signal_build`
- 6 个 CLI 脚本
  - `run_alpha_bof_build.py`
  - `run_alpha_tst_build.py`
  - `run_alpha_pb_build.py`
  - `run_alpha_cpb_build.py`
  - `run_alpha_bpb_build.py`
  - `run_alpha_signal_build.py`

当前 live formal DB 可回查事实：

- 5 个 trigger 库均只保留 `alpha_run / alpha_work_queue / alpha_checkpoint / alpha_trigger_event / alpha_trigger_profile`
- `alpha_signal.duckdb` 只保留 `alpha_signal / alpha_signal_run / alpha_signal_work_queue / alpha_signal_checkpoint`
- 未发现 `opportunity_score / grade / risk_reward_ratio / quality_flag / neutrality / 16-cell` 等正式字段
- `position_run.alpha_source_path` 仍指向 `H:\Lifespan-data\astock_lifespan_alpha\alpha\alpha_signal.duckdb`

## 5. 下一轮最小升级目标

下一轮实现目标默认冻结为：

- 不是引入 scoring engine
- 不是恢复历史 PAS 评分体系
- 不是把 `16-cell` 拉回当前系统

下一轮只考虑：

- trigger registry/readout 命名统一
- runner / script / spec 名称对齐
- `alpha_signal` 与五个 trigger 的追溯面补齐文档契约

换句话说，下一轮要做的是：

- 治理升级

而不是：

- 评分引擎升级

## 6. 与现有规格的阅读顺序

阅读 `alpha(PAS)` 当前合同与下一轮边界时，按下面顺序：

1. `docs/02-spec/03-alpha-pas-trigger-semantic-spec-v1-20260419.md`
2. `docs/02-spec/04-alpha-signal-aggregation-spec-v1-20260419.md`
3. `docs/02-spec/28-alpha-pas-upgrade-boundary-and-legacy-delta-selection-v1-20260424.md`
4. `docs/02-spec/27-lineage-lessons-malf-alpha-pas-core-v1-20260424.md`

其中：

- `03 / 04` 说明当前 trigger 合同
- `28` 说明哪些历史 PAS 能力暂不吸收，哪些成为下一轮最小升级入口
- `27` 说明更高层的 lineage lessons 与 `MALF+alpha(PAS)` 核心路线

## 7. 冻结结论

本文冻结以下结论：

1. 当前 `alpha` 已放行，但其角色仍是 `trigger ledger producer`。
2. 当前 `alpha` 不是完整 `PAS scoring engine`。
3. 历史 `opportunity_score / grade / risk_reward_ratio / quality_flag / neutrality / readout / condition matrix` 不进入当前正式合同。
4. `16-cell` 只作为前代历史素材登记，当前系统不存在，也不进入下一轮治理候选。
5. 下一轮最小实现方向是治理升级，不是评分引擎升级。
