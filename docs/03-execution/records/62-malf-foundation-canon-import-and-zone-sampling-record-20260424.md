# 批次 62 记录：MALF 地基 Canon、包入口修复与 zone sampling 收口

- 日期：`2026-04-24`
- 对应卡片：`docs/03-execution/62-malf-foundation-canon-import-and-zone-sampling-card-20260424.md`

## 1. 记录

用户确认项目不是单个 Codex worktree，而是由五根目录共同承担：

- `H:\astock_lifespan-alpha`
- `H:\Lifespan-data`
- `H:\Lifespan-report`
- `H:\Lifespan-Validated`
- `H:\Lifespan-temp`

因此本轮修正执行口径：

- 所有代码与文档改动落在 `H:\astock_lifespan-alpha`
- 所有真实数据验证显式读取 `H:\Lifespan-data`
- 所有报告与临时目录使用对应 H 盘根
- Python 使用 `D:\miniconda\py310`

## 2. 工程记录

`malf/__init__.py` 不再导入 `malf.audit`。audit 仍作为治理/开发入口存在，但调用方必须显式从 `astock_lifespan_alpha.malf.audit` 导入。

`zone_coverage` 的 Card 61 证据已表明全量 formal target 具备四区；本轮只调整 sample 选择，避免 sample coverage 误触发软观察。

## 3. 偏差

本轮开始前，`H:\astock_lifespan-alpha` 已存在 `audit.py` 与 `test_audit.py` 的未提交改动，内容为 audit required tables 的 symbol chunk materialization。该改动与本轮 sampling 修复共享文件，本轮只在其上追加最小逻辑，不回滚或覆盖。
