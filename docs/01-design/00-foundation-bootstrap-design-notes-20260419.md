# 阶段一基础重构设计说明

日期：`2026-04-19`
状态：`生效中`

## 1. 设计目标

阶段一不是业务实现阶段，而是新系统的落地底盘阶段。

这一阶段只解决三件事：

1. 把仓库从旧系统上下文里切出来，形成独立仓库与独立环境。
2. 把新系统的正式边界冻结为 `data -> malf -> alpha -> position -> portfolio_plan -> trade -> system`。
3. 把后续可持续重构所需的最小运行接口、路径契约与测试基线先立住。

## 2. 为什么先做这些

如果没有阶段一，后面的 `MALF`、`alpha` 和下游链路会直接陷入三类失控：

- 路径失控：代码仍然会混用旧仓库、旧数据根目录和旧账本空间。
- 边界失控：`structure`、`filter` 虽然口头上被移除，但代码和脚本仍可能继续引用。
- 交付失控：后续阶段没有统一 runner 命名、没有最小验收、没有最小测试基线。

因此阶段一必须先把“能不能作为一个新系统继续往下做”这个问题解决掉，而不是急着写业务逻辑。

## 3. 阶段一设计产物

### 3.1 五根目录工作区契约

正式工作区固定为五根目录：

- 仓库根：`H:\astock_lifespan-alpha`
- 数据根：`H:\Lifespan-data`
- 报告根：`H:\Lifespan-report`
- 临时根：`H:\Lifespan-temp`
- 验证根：`H:\Lifespan-Validated`

对应设计要求：

- 仓库根只放代码、测试、文档与治理资产。
- 新系统正式账本统一落在 `H:\Lifespan-data\astock_lifespan_alpha\`。
- `temp/report/validated` 也必须按模块展开独立命名空间，不能继续与旧系统混用。

### 3.2 模块边界收缩

新系统正式模块固定为：

- `core`
- `data`
- `malf`
- `alpha`
- `position`
- `portfolio_plan`
- `trade`
- `system`

设计约束：

- `structure` 与 `filter` 不再是正式模块。
- 阶段一即便不实现业务，也必须先通过测试约束它们不再被源码和脚本引用。

### 3.3 Runner Stub 先行

阶段一 runner 只允许提供“可调用、可验收、不可冒充业务完成”的 stub。

这样做的原因：

- 先冻结运行入口命名。
- 先冻结目标账本路径。
- 明确告诉后续阶段当前仍未进入业务物化。

因此返回结果必须显式包含：

- `runner_name`
- `module_name`
- `status=stub`
- `phase=foundation_bootstrap`
- `target_path`

## 4. 本阶段明确不做什么

阶段一不负责：

- 实现 `MALF` 语义逻辑。
- 实现五类 PAS 触发器。
- 实现 `position / portfolio_plan / trade / system` 的正式业务账本。
- 恢复任何旧 `structure/filter` 生产语义。

这些工作必须进入后续 card，再按文档治理闭环推进。

## 5. 对文档治理的直接要求

阶段一完成后，仓库必须立即补齐中文治理闭环。

原因很直接：

- 目前代码已经先落地，但缺少与之对应的设计、规格、执行记录。
- 如果继续在这种状态下推进第二阶段，后续所有“为什么这样做、是否已验收、哪些是假实现”都会失真。

因此从本文件开始，后续正式文档统一使用中文，并统一收敛到：

`01-design -> 02-spec -> 03-execution`

## 6. 阶段一验收定义

阶段一被视为完成，至少需要满足：

1. 新仓库可独立安装与运行测试。
2. 五根目录契约可解析，并创建新命名空间目录。
3. `structure/filter` 不再出现在正式源码和脚本依赖中。
4. `malf/alpha/position` 的最小 runner 入口可调用。
5. 阶段一自身拥有完整中文补记：`card + evidence + record + conclusion`。
