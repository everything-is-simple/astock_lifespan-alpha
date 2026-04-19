# 阶段四批次 16 position 最小账本与 runner 规格冻结证据

证据编号：`16`
日期：`2026-04-19`

## 1. 命令

```text
rg -n "position_run|position_work_queue|position_checkpoint|position_candidate_audit|position_capacity_snapshot|position_sizing_snapshot" docs/02-spec
```

## 2. 关键结果

- 六张正式表都已写入 `position` 规格。
- queue / checkpoint / replay 与 `PositionRunSummary` 的职责边界已冻结。

## 3. 产物

- `docs/02-spec/06-position-minimal-ledger-and-runner-spec-v1-20260419.md`
