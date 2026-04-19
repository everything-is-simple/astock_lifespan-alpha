# astock-lifespan-alpha 重构总方案 v1

日期：`2026-04-19`
状态：`冻结`

## 0. 文档定位

本文档是 `astock_lifespan-alpha` 的正式重构总方案。

它回答四个问题：

1. 新系统到底是什么。
2. `lifespan-0.01` 里哪些东西继承，哪些东西废止。
3. 哪些数据库可以复用，哪些必须重建。
4. 技术栈、模块主链路与阶段交付顺序是什么。

## 1. 新系统定义

新系统定义为：

> `lifespan-0.01 - structure - filter`

这句话的正式含义是：

- 继承 `lifespan-0.01` 的主线分层思想与下游模块序列。
- 明确移除 `structure` 与 `filter` 作为正式真值层。
- 从 `MALF` 开始重建新的语义账本宇宙。

正式模块集合固定为：

- `core`
- `data`
- `malf`
- `alpha`
- `position`
- `portfolio_plan`
- `trade`
- `system`

正式主链路固定为：

```text
data -> malf -> alpha -> position -> portfolio_plan -> trade -> system
```

## 2. 系统真值边界

新系统的真值边界冻结如下：

- `data`：客观事实层
- `malf`：唯一正式市场结构真值层
- `alpha`：PAS 五触发器信号层
- `position`：持仓物化层
- `portfolio_plan`：组合决策桥接层
- `trade`：执行运行层
- `system`：最终读出与编排层

明确废止：

- `structure` 不再是正式上游。
- `filter` 不再是正式上游。

它们不是“暂时停用”，而是被移出新系统正式架构。

## 3. 五根目录工作区规格

正式工作区固定为：

- 仓库根：`H:\astock_lifespan-alpha`
- 数据根：`H:\Lifespan-data`
- 报告根：`H:\Lifespan-report`
- 临时根：`H:\Lifespan-temp`
- 验证根：`H:\Lifespan-Validated`

职责冻结为：

- 仓库根：代码、测试、文档、治理资产、脚本
- 数据根：正式数据库与长期数据资产
- 报告根：人类可读报告与导出物
- 临时根：回放缓存、pytest 临时目录、临时中间产物
- 验证根：已接受的快照、验证产物与证据副本

## 4. 数据库重构决策

正式数据库策略冻结为：

> 旧 `raw/base` 可以选择性复用；从 `MALF` 往下的正式账本一律按新系统语义重建

### 4.1 可复用层

仅允许把客观事实层当作输入来源：

- `raw_market`
- `market_base`
- 其他与旧 `MALF` 语义无关、且本质上属于客观事实的源账本

### 4.2 不可复用层

以下旧账本不能作为新系统正式真值继承：

- 旧 `malf`
- 旧 `alpha`
- 旧 `position`
- 旧 `portfolio_plan`
- 旧 `trade`
- 旧 `system`

原因冻结为：

> 旧 `MALF` 语义被判定不再可信，因此所有建立在其上的下游语义账本一并失去正式继承资格

### 4.3 新系统账本命名空间

新系统正式账本统一落在：

```text
H:\Lifespan-data\astock_lifespan_alpha\
```

首版数据库家族冻结为：

- `malf\malf_day.duckdb`
- `malf\malf_week.duckdb`
- `malf\malf_month.duckdb`
- `alpha\alpha_bof.duckdb`
- `alpha\alpha_tst.duckdb`
- `alpha\alpha_pb.duckdb`
- `alpha\alpha_cpb.duckdb`
- `alpha\alpha_bpb.duckdb`
- `alpha\alpha_signal.duckdb`
- `position\position.duckdb`
- `portfolio_plan\portfolio_plan.duckdb`
- `trade\trade.duckdb`
- `system\system.duckdb`

## 5. 技术栈决策

首版实现技术栈冻结为：

> `Python + DuckDB + Arrow`

职责拆分固定为：

- `DuckDB`：正式账本存储与查询
- `Arrow`：批量表格交换格式
- `Python`：编排、runner、回放、断点、工作队列与领域执行

### 5.1 DuckDB 责任

- 正式数据库存储
- SQL 查询、过滤、关联与聚合
- 历史区间加载
- profile / percentile 持久化
- checkpoint 与队列表物化

### 5.2 Arrow 责任

- 模块间批量交换
- `MALF` 中间结果交换
- `alpha` 输入输出交换
- 模块间表格移交

### 5.3 Python 责任

- runner 组织
- 回放流程
- checkpoint 处理
- 工作队列管理
- 领域状态迁移
- 正式构建顺序控制

### 5.4 pandas 使用限制

`pandas` 只能作为局部辅助工具，不得成为正式数据模型。

因此：

- 账本契约不能依赖 DataFrame 形状
- 领域语义不能靠 pandas 约定来定义
- 核心 `MALF` 语义不能写成 pandas-first

### 5.5 未来迁移约束

首版实现必须为未来迁移到 `Go + DuckDB` 预留空间，因此：

- 领域语义要与语言无关
- Schema 契约要与语言无关
- runner 契约要与语言无关
- 优先 Arrow-first，而不是 pandas-first 的偷懒实现

## 6. MALF 正式重构规格

`malf` 是新系统唯一正式市场结构真值层。

它只处理价格结构事实，不处理：

- 交易动作
- 概率预测
- 持仓建议
- 跨周期决策逻辑

### 6.1 三周期独立账本

`MALF` 固定拆为三个相互独立的正式账本：

- `malf_day`
- `malf_week`
- `malf_month`

每个周期都必须独立拥有：

- runner
- work_queue
- checkpoint
- rebuild flow
- life statistics
- sample genealogy

任何一个周期都不允许拿另一个周期来定义自己的 life 尺度。

### 6.2 最小生命表达

正式最小生命表达冻结为：

```text
Life = (direction, new_count, no_new_span, life_state)
```

其中：

- `direction`：当前波方向
- `new_count`：严格新高/新低替换次数
- `no_new_span`：自最新一次新高/新低之后连续未创新的 bar 数
- `life_state`：正式生命边界状态

### 6.3 统一波段位置表达

正式波段位置坐标冻结为：

```text
WavePosition = (direction, update_rank, stagnation_rank, life_state)
```

它是描述性坐标，不携带交易动作含义。

### 6.4 核心语义规则

正式规则冻结为：

- `new_count` 只记录严格新值替换
- 上升波只统计新 `HH`
- 下降波只统计新 `LL`
- `HL/LH` 不计入 `new_count`
- 近似、相等、失败突破都不计入
- 出现新 `HH/LL` 时 `no_new_span` 归零
- 只有旧波尚未被破坏时，`no_new_span` 才继续增长
- `life_state` 固定为 `alive / broken / reborn`
- `break != confirmation`

### 6.5 reborn 规则

`reborn` 被正式保留，其含义冻结为：

> 旧波已经被破坏，但新方向第一次有效 `new_count` 尚未确认前，处于一种正式中间生命态，称为 `reborn`

### 6.6 guard 规则

guard 规则冻结为：

> 使用当前波内最近一个仍然有效的同波结构锚点作为 guard anchor

具体为：

- 上升波 guard = 最近有效 `HL`
- 下降波 guard = 最近有效 `LH`
- 当该锚点被破坏时，旧波终止

### 6.7 MALF 最小输出表

每个 `MALF` 周期数据库至少包含：

- `malf_run`
- `malf_work_queue`
- `malf_checkpoint`
- `malf_pivot_ledger`
- `malf_wave_ledger`
- `malf_state_snapshot`
- `malf_wave_scale_snapshot`
- `malf_wave_scale_profile`

### 6.8 alpha 面向快照

`malf_wave_scale_snapshot` 是面向 `alpha` 的正式读模型。

最小字段集冻结为：

- `symbol`
- `timeframe`
- `bar_dt`
- `direction`
- `wave_id`
- `new_count`
- `no_new_span`
- `life_state`
- `update_rank`
- `stagnation_rank`
- `wave_position_zone`

### 6.9 波段区间分区

`wave_position_zone` 固定为四个区域：

- `early_progress`
- `mature_progress`
- `mature_stagnation`
- `weak_stagnation`

### 6.10 rank 计算样本约束

`update_rank` 与 `stagnation_rank` 采用经验百分位构造。

样本约束冻结为：

- 同一标的
- 同一周期
- 同一方向
- 以完整历史波作为样本单位

## 7. Alpha 正式重构规格

`alpha` 重构为日线 PAS 五触发器体系：

- `bof`
- `tst`
- `pb`
- `cpb`
- `bpb`

### 7.1 五账本独立

每个 trigger 都必须拥有独立正式账本：

- `alpha_bof`
- `alpha_tst`
- `alpha_pb`
- `alpha_cpb`
- `alpha_bpb`

并独立拥有：

- `run`
- `work_queue`
- `checkpoint`
- `trigger_event`
- `trigger_profile`

### 7.2 Alpha 输入边界

阶段一后的正式输入边界固定为：

- `market_base_day`
- `malf_day.malf_wave_scale_snapshot`

阶段一不恢复旧 `structure/filter/family/formal_signal` 作为上游权威。

### 7.3 Alpha 汇总账本

必须新增正式汇总账本：

- `alpha_signal`

它的职责是：

- 统一五类 trigger 输出
- 成为 `position` 的唯一正式上游输入

最小字段集冻结为：

- `signal_nk`
- `symbol`
- `signal_date`
- `trigger_type`
- `formal_signal_status`
- `source_trigger_db`
- `source_trigger_event_nk`
- `wave_id`
- `direction`
- `new_count`
- `no_new_span`
- `life_state`
- `update_rank`
- `stagnation_rank`
- `wave_position_zone`

## 8. Position / Portfolio Plan / Trade / System

### 8.1 Position

`position` 继承旧系统的实施纪律，但更换正式上游。

它只能读取：

- `alpha_signal`

不得直接把五个 trigger 账本当作生产输入。

### 8.2 Portfolio Plan

`portfolio_plan` 保留为正式独立层，不删除、不并入其他层。

### 8.3 Trade 与 System

在前期阶段里：

- `trade` 暂时保留稳定接口
- `system` 暂时保留稳定接口

在上游真值链条稳定之前，不要求深度重构。

## 9. 分阶段交付

### 阶段一：仓库与基础底盘引导

任务：

- 初始化新仓库
- 绑定 git remote
- 建立新的 `pyproject.toml`
- 建立新的 `.venv`
- 收缩 `.codex` 治理骨架
- 冻结五根目录解析
- 建立新数据命名空间

验收：

- 仓库可独立运行
- 环境已隔离
- 五根目录解析正确
- 旧 `raw/base` 可作为源输入读取

### 阶段二：MALF 冻结与构建

任务：

- 冻结 `MALF` 文本规格
- 冻结 `MALF` 图版规格
- 实现日/周/月账本与 schema
- 实现 runner、queue、checkpoint 与 rebuild flow
- 实现 `malf_wave_scale_snapshot` 与 `malf_wave_scale_profile`
- 实现 `MALF` 语义测试

验收：

- 三周期账本彼此独立运行
- `reborn` 与 guard 规则正确执行
- 波段位置坐标被稳定物化

### 阶段三：Alpha 五账本构建

任务：

- 实现五类 PAS trigger 账本
- 实现独立 runner、queue、checkpoint
- 把 alpha 输入绑定到 `market_base_day + malf_day`
- 实现 `alpha_signal`

验收：

- 任一 trigger 可独立运行
- 五类 trigger 能稳定汇总进 `alpha_signal`
- 输出足够稳定，可供 `position` 消费

### 阶段四：Position 接口切换

任务：

- 把 `position` 正式改绑到 `alpha_signal`
- 保留 `portfolio_plan` 中间桥层
- 验证最小主线连续性

验收：

- `position` 只依赖 `alpha_signal`
- `portfolio_plan` 桥层保留
- 新主线真值链稳定

## 10. 测试与验收

### 10.1 基础测试

- 新仓库使用独立环境
- 五根目录可正确解析
- 旧 `raw/base` 可被读取为源输入

### 10.2 MALF 语义测试

- 只有新 `HH` 才增加上升波 `new_count`
- 只有新 `LL` 才增加下降波 `new_count`
- `no_new_span` 只在“未继续、未破坏”时增长
- `break` 不直接等于新方向确认
- `reborn` 出现在旧波破坏后、新方向确认前
- 最近有效 `HL/LH` 被用作 guard anchor

### 10.3 MALF 独立性测试

- `malf_day / malf_week / malf_month` 可独立构建
- 一个周期的回放或重建不得污染另两个周期

### 10.4 Alpha 测试

- 任一 PAS trigger 账本可独立运行
- 每个 trigger 支持 checkpoint 与 replay
- `alpha_signal` 可稳定汇总五类输出

### 10.5 Position 接口测试

- `position` 仅依赖 `alpha_signal` 即可生成最小候选集

## 11. 文档输出要求

本仓库正式文档家族固定为：

- `docs/01-design/`：设计理由
- `docs/02-spec/`：正式规格
- `docs/03-execution/`：执行闭环

从本方案开始，正式文档统一使用中文。

## 12. 冻结结论

本文档冻结以下重构结论：

1. 新系统定义为 `lifespan-0.01 - structure - filter`。
2. 正式主链路为 `data -> malf -> alpha -> position -> portfolio_plan -> trade -> system`。
3. 旧 `raw/base` 可选择性复用。
4. 旧 `MALF` 及其全部下游语义账本不得作为新系统真值继承。
5. 首版技术栈为 `Python + DuckDB + Arrow`。
6. `MALF` 是唯一正式市场结构真值层。
7. `MALF` 保留 `reborn`。
8. `alpha` 重构为五个 PAS 触发器账本加一个 `alpha_signal` 汇总账本。
9. `position` 只读取 `alpha_signal`。
