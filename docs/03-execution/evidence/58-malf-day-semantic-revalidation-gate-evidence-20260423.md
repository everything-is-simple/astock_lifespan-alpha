# 批次 58 证据：MALF day 纯语义重验 gate

- 日期：`2026-04-23`
- 对应卡片：`docs/03-execution/58-malf-day-semantic-revalidation-gate-card-20260423.md`

## 1. 执行命令

```powershell
python scripts/malf/audit_malf_day_semantics.py --run-id day-a1c965e1f7a9 --sample-count 12
```

## 2. 审计产物

- `report_id = malf-day-semantic-audit-b7ea18c043f9`
- JSON summary：
  - `H:\Lifespan-report\astock_lifespan_alpha\malf\malf-day-semantic-audit-b7ea18c043f9\malf-day-semantic-audit-b7ea18c043f9.json`
- Markdown summary：
  - `H:\Lifespan-report\astock_lifespan_alpha\malf\malf-day-semantic-audit-b7ea18c043f9\malf-day-semantic-audit-b7ea18c043f9.md`
- 审计 DuckDB：
  - `H:\Lifespan-report\astock_lifespan_alpha\malf\malf-day-semantic-audit-b7ea18c043f9\malf-day-semantic-audit-b7ea18c043f9.duckdb`
- 图表目录：
  - `H:\Lifespan-report\astock_lifespan_alpha\malf\malf-day-semantic-audit-b7ea18c043f9\charts\`

## 3. run 选择与回落

- `requested_run_id = day-a1c965e1f7a9`
- `effective_run_id = day-fc56ff5e5441`
- 回落原因：
  - `day-a1c965e1f7a9` 虽为较新的 `completed` run，但 `symbols_updated = 0`
  - `inserted_state_snapshots = 0`
  - 核心 MALF 账本未物化
  - 因此按规格回落到最新一个已物化核心账本的 `completed` run

## 4. live 审计核心结果

- `target_started_at = 2026-04-20T21:32:49.372445`
- `target_finished_at = 2026-04-20T22:11:54.249128`
- `target_symbol_total = 5501`
- `target_symbol_completed = 5501`
- `target_snapshot_rows = 8138049`
- `target_wave_rows = 3300393`
- `running_queue_count = 14`

## 5. stale bookkeeping 记录

- stale `running` run：
  - `day-3343b24d0f0b`，`symbols_completed = 540 / 5501`
  - `day-d696fdcd4774`，`symbols_completed = 0 / 0`

以上 stale run 只做 bookkeeping 记录，不参与语义评分。

## 6. 硬规则结果

7 项硬规则全部 `passed`，`violation_count = 0`：

1. `new_count_transition_rule`
2. `no_new_span_transition_rule`
3. `wave_id_break_rule`
4. `new_wave_reborn_rule`
5. `reborn_to_alive_rule`
6. `guard_update_pivot_rule`
7. `zone_classification_rule`

## 7. 软观察结果

4 项软观察全部触发 flag：

- `zone_coverage = 2`
- `reborn_median_bar_count = 1.0`
- `single_bar_reborn_share = 0.8085`
- `guard_churn_p90 = 0.75`

## 8. 标准导出表

- `wave_summary = 3300393`
- `state_snapshot_sample = 428`
- `break_events = 3296892`
- `reborn_windows = 3296892`

以上表均已写入审计 DuckDB。

## 9. 样本图

固定导出 `12` 段样本图，位于：

- `H:\Lifespan-report\astock_lifespan_alpha\malf\malf-day-semantic-audit-b7ea18c043f9\charts\`

代表性样本：

- `up_stagnation-01.png`
  - wave：`601288.SH:day:wave:0004`
  - 观察：停滞与 reborn 窗口偏短，zone 覆盖偏稀
- `transition_to_down-01.png`
  - wave：`601880.SH:day:wave:0919`
  - 观察：break 与反向重建主干成立，但 guard 表达仍偏 bar-driven

## 10. authority material 登记

本轮对照材料：

- `docs/02-spec/01-malf-wave-scale-semantic-spec-v1-placeholder-20260419.md`
- `H:\Lifespan-Validated\malf-six\001.png` 至 `006.png`
- `H:\Lifespan-Validated\MALF_终极定义文件_六图合并版.pdf`
- `H:\Lifespan-Validated\MALF_波段标尺正式语义规格_v1_图版_6页稿.pdf`
- `H:\Lifespan-Validated\MALF_波段标尺正式语义规格_v1_图版_18页稿.pdf`
- `H:\Lifespan-Validated\MALF_第四战场_波段标尺正式语义规格_v1_图版.pdf`
- `H:\Lifespan-Validated\MALF_终极定义文件_与chatgpt聊天.pdf`
