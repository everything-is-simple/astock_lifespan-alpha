# 阶段二批次 03 MALF 图版规格冻结证据

证据编号：`03`
日期：`2026-04-19`

## 1. 命令

```text
.\.venv\Scripts\python -m pytest -q
```

## 2. 关键结果

- `tests/unit/docs/test_malf_specs.py` 通过，证明图版规格已声明“文本优先于图版”。
- 图版规格已包含“文本条款 -> 图版位置对照”表，并明确要求显式表达 `reborn`、guard、`wave_position_zone`。
- 三份现有 PDF 图稿已经被统一为“18 页稿主来源、6 页稿摘要来源、旧图稿仅历史参考”的整理口径。

## 3. 产物

- `docs/02-spec/02-malf-wave-scale-diagram-edition-placeholder-20260419.md`
