# astock_lifespan-alpha Reconstruction Master Plan v1

## 0. Document Position

This document is the master reconstruction plan for `astock_lifespan-alpha`.

It serves as the top-level implementation and governance baseline for the new system.

This document answers four questions:

1. What the new system is
2. Which parts of `lifespan-0.01` are inherited and which are discarded
3. Which databases may be reused and which must be rebuilt
4. Which technical stack and phased delivery order the new system must follow


## 1. System Definition

The new system is defined as:

> `lifespan-0.01 - structure - filter`


This means the new system preserves the formal core skeleton of `lifespan-0.01`, while completely removing `structure` and `filter` from the mainline.

The formal module set is fixed as:

- `core`
- `data`
- `malf`
- `alpha`
- `position`
- `portfolio_plan`
- `trade`
- `system`


The formal mainline is fixed as:

```text
data -> malf -> alpha -> position -> portfolio_plan -> trade -> system
```


## 2. System Boundary

The new system keeps the original layered mindset of `lifespan-0.01`, but resets the truth boundary.

The new truth boundary is:

- `data` is the objective fact layer
- `malf` is the only market structure truth layer
- `alpha` is the PAS five-trigger signal layer
- `position` is the position materialization layer
- `portfolio_plan` is the portfolio decision bridge layer
- `trade` is the execution runtime layer
- `system` is the final readout and orchestration layer


`structure` and `filter` are no longer formal truth layers in the new system.

They are not "temporarily disabled".

They are removed from the formal system architecture.


## 3. Five-Root Workspace Contract

The new system continues to use the existing five-root layout.

The formal roots are:

- repo root: `H:\astock_lifespan-alpha`
- data root: `H:\Lifespan-data`
- report root: `H:\Lifespan-report`
- temp root: `H:\Lifespan-temp`
- validated root: `H:\Lifespan-Validated`


Their roles remain distinct:

- repo root stores code, docs, tests, governance, and formal scripts
- data root stores formal databases and long-lived data assets
- report root stores human-readable reports and exported outputs
- temp root stores replay scratch work, pytest temp data, caches, and temporary artifacts
- validated root stores accepted validation snapshots and evidence assets


## 4. Database Reconstruction Decision

The database strategy is formally frozen as:

> old `raw/base` may be selectively reused; from `MALF` onward, all formal ledgers must be rebuilt as new-system ledgers


### 4.1 Reusable Fact Layer

The following old-system layers may be selectively reused as objective source ledgers:

- `raw_market`
- `market_base`
- any other source layer that is objectively factual and not dependent on old MALF semantics


These are source-of-fact inputs, not downstream semantic truth.


### 4.2 Non-Reusable Semantic Layer

The following old-system databases must not be treated as formal truth ledgers for the new system:

- old `malf`
- old `alpha`
- old `position`
- old `portfolio_plan`
- old `trade`
- old `system`


The key reason is:

> old `malf` is judged to be semantically wrong, therefore all ledgers built on top of it lose formal inheritance eligibility


### 4.3 New-System Ledger Universe

The new system must create its own independent ledger universe from MALF onward.

Recommended path namespace:

```text
H:\Lifespan-data\astock_lifespan_alpha\
```


Recommended formal database family:

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


## 5. Technical Stack Decision

The first-edition implementation stack is formally frozen as:

> `Python + DuckDB + Arrow`


Its responsibility split is fixed as:

- `DuckDB` manages formal ledgers
- `Arrow` manages batch exchange
- `Python` manages orchestration, runner flow, replay, checkpoint, and domain execution


### 5.1 DuckDB Responsibility

DuckDB is the formal ledger and query engine.

It is responsible for:

- formal database storage
- SQL filtering and joins
- aggregation
- replay range loading
- profile and percentile persistence
- checkpoint and queue-side ledger materialization


### 5.2 Arrow Responsibility

Arrow is the standard batch exchange format between modules and execution stages.

It is responsible for:

- bar batch transport
- MALF intermediate batch exchange
- alpha trigger input and output batch exchange
- module-to-module tabular handoff


### 5.3 Python Responsibility

Python is the orchestration and execution language for the first edition.

It is responsible for:

- runner organization
- replay flow
- checkpoint handling
- work queue handling
- domain state transitions
- formal build sequencing


### 5.4 pandas Restriction

`pandas` may be used only as a local auxiliary tool.

It must not become the formal system data model.

This means:

- ledger contracts must not depend on pandas-specific structures
- domain semantics must not be defined by DataFrame shape
- core MALF logic must not be pandas-first


### 5.5 Future Migration Constraint

The first edition must be written under a future migration target of:

> `Go + DuckDB`


Therefore:

- domain semantics must remain language-independent
- schema contracts must remain language-independent
- runner contracts must remain language-independent
- Arrow-first exchange must be preferred over pandas-first implementation shortcuts


## 6. MALF Formal Reconstruction

`malf` becomes the only formal market structure truth layer.

It no longer delegates truth authority to `structure` or `filter`.

`malf` only handles price-structure facts.

It does not handle:

- trading action
- probability forecast
- position advice
- cross-timeframe decision logic


### 6.1 Three Independent Timeframe Ledgers

MALF is split into three fully independent formal ledgers:

- `malf_day`
- `malf_week`
- `malf_month`


Each timeframe must have independent:

- runner
- work queue
- checkpoint
- rebuild flow
- life statistics
- sample genealogy


No timeframe is allowed to define its life ruler using another timeframe.


### 6.2 MALF Minimal Life Expression

The formal minimal life expression is:

```text
Life = (direction, new_count, no_new_span, life_state)
```


Where:

- `direction` is current wave direction
- `new_count` is the strict new-value replacement count
- `no_new_span` is the count of consecutive bars without new continuation since the latest `new_count`
- `life_state` is the formal life boundary state


### 6.3 MALF Unified Wave Position

The unified formal wave ruler coordinate is:

```text
WavePosition = (direction, update_rank, stagnation_rank, life_state)
```


This position is descriptive, not action-bearing.


### 6.4 MALF Core Semantic Rules

Formal rules are frozen as:

- `new_count` records only strict new-value replacement
- upward waves count only new `HH`
- downward waves count only new `LL`
- `HL/LH` do not count as `new_count`
- approximate, equal, or failed-break values do not count
- `no_new_span` resets to zero when a new `HH/LL` occurs
- `no_new_span` increases only while the current wave remains unbroken
- `life_state` is fixed to `alive / broken / reborn`
- `break != confirmation`


### 6.5 Reborn Decision

`reborn` is formally preserved.

Its meaning is frozen as:

> after the old wave has been broken, but before the first valid opposite-direction `new_count` confirms the new wave, the new life exists in a formal intermediate state called `reborn`


### 6.6 Guard Rule

The formal guard rule is frozen as:

> use the latest valid same-wave structural anchor as the guard anchor


This means:

- upward wave guard = latest valid `HL`
- downward wave guard = latest valid `LH`
- once this anchor is broken, the old wave is terminated


### 6.7 MALF Formal Output Tables

Each MALF timeframe database must at minimum contain:

- `malf_run`
- `malf_work_queue`
- `malf_checkpoint`
- `malf_pivot_ledger`
- `malf_wave_ledger`
- `malf_state_snapshot`
- `malf_wave_scale_snapshot`
- `malf_wave_scale_profile`


### 6.8 MALF Wave Scale Snapshot

`malf_wave_scale_snapshot` is the formal alpha-facing read model.

Its minimum field set is frozen as:

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


### 6.9 MALF Wave Position Zone

`wave_position_zone` is fixed to four areas:

- `early_progress`
- `mature_progress`
- `mature_stagnation`
- `weak_stagnation`


### 6.10 MALF Rank Construction

`update_rank` and `stagnation_rank` are implemented as empirical percentile positions.

The sample contract is frozen as:

- same symbol
- same timeframe
- same direction
- complete historical wave as the sample unit


## 7. Alpha Formal Reconstruction

`alpha` is reconstructed as a daily PAS five-trigger system.

The five triggers are:

- `bof`
- `tst`
- `pb`
- `cpb`
- `bpb`


### 7.1 Five Independent Trigger Ledgers

Each trigger must independently run, build, and maintain its own formal ledger:

- `alpha_bof`
- `alpha_tst`
- `alpha_pb`
- `alpha_cpb`
- `alpha_bpb`


Each trigger ledger must independently own:

- `run`
- `work_queue`
- `checkpoint`
- `trigger_event`
- `trigger_profile`


### 7.2 Alpha Input Boundary

The PAS five-trigger system reads only:

- `market_base_day`
- `malf_day.malf_wave_scale_snapshot`


The new system does not restore old `structure/filter/family/formal_signal` as upstream authority in phase one.


### 7.3 Alpha Signal Summary Ledger

A new formal summary ledger must be created:

- `alpha_signal`


Its role is:

- standardize five-trigger output
- become the only formal upstream input for `position`


Its minimum field set is frozen as:

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


## 8. Position, Portfolio Plan, Trade, System

### 8.1 Position

`position` inherits implementation discipline from `lifespan-0.01`, but changes its upstream contract.

`position` must read only:

- `alpha_signal`


It must not directly read the five trigger ledgers as formal production input.


### 8.2 Portfolio Plan

`portfolio_plan` remains a formal independent layer.

It is not deleted and not merged into other layers.

It remains between `position` and `trade`.


### 8.3 Trade and System

In phase one:

- `trade` keeps stable upstream interfaces
- `system` keeps stable upstream interfaces


They do not need full deep reconstruction before the upstream truth chain is stabilized.


## 9. Phase Delivery Plan

### Phase 1: Repository and Foundation Bootstrap

Tasks:

- initialize the new repository
- bind git remote
- create the new `pyproject.toml`
- create the new `.venv`
- transplant and reduce the governance skeleton from `.codex`
- freeze five-root path parsing
- establish the new data-root namespace for rebuilt ledgers


Acceptance:

- repository can run independently
- environment is isolated
- five-root paths resolve correctly
- old raw/base can be read as source inputs


### Phase 2: MALF Freeze and Build

Tasks:

- freeze MALF textual specification
- freeze MALF diagram edition
- implement MALF day/week/month bootstrap and schema
- implement MALF runner, queue, checkpoint, and rebuild flow
- implement `malf_wave_scale_snapshot` and `malf_wave_scale_profile`
- implement MALF semantic tests


Acceptance:

- day/week/month ledgers run independently
- `reborn` and guard rules are correctly enforced
- wave ruler coordinates are materialized


### Phase 3: Alpha Five-Ledger Build

Tasks:

- implement the five PAS trigger ledgers
- implement independent runner, queue, checkpoint logic
- bind alpha input to `market_base_day + malf_day`
- implement `alpha_signal`


Acceptance:

- any trigger can run independently
- all five triggers can be summarized into `alpha_signal`
- standardized output is stable enough for `position`


### Phase 4: Position Interface Cutover

Tasks:

- rebind `position` to `alpha_signal`
- preserve `portfolio_plan` as the middle bridge
- validate minimal mainline continuity


Acceptance:

- `position` can consume only `alpha_signal`
- `portfolio_plan` bridge remains intact
- new upstream truth chain is stable


## 10. Testing and Acceptance

### 10.1 Foundation Tests

- new repository creates and uses its own environment
- five-root layout resolves correctly
- old raw/base databases can be read from the new system


### 10.2 MALF Semantic Tests

- only new `HH` increments upward `new_count`
- only new `LL` increments downward `new_count`
- `no_new_span` increments only while continuation is absent and life is unbroken
- `break` does not directly confirm a new direction
- `reborn` exists after old-wave break and before new-wave confirmation
- latest valid `HL/LH` is used as the formal guard anchor


### 10.3 MALF Independence Tests

- `malf_day`, `malf_week`, and `malf_month` can be built independently
- replay or rebuild on one timeframe does not contaminate the other two


### 10.4 Alpha Tests

- any PAS trigger ledger can run independently
- each trigger supports checkpoint and replay
- `alpha_signal` consistently summarizes five-trigger output


### 10.5 Position Interface Tests

- `position` can create minimal candidates using only `alpha_signal`


### 10.6 Phase-One Acceptance

Phase one is accepted only when:

- the system no longer depends on `structure/filter`
- MALF textual spec and diagram spec are semantically aligned
- the chain `data -> malf -> alpha_signal -> position -> portfolio_plan` is formally stable


## 11. Document Outputs

The following document family must be created as the new baseline:

- `docs/01-design/` for design rationale
- `docs/02-spec/` for formal specifications
- `docs/03-execution/` for execution cards


This master plan belongs in:

- `docs/02-spec/`


The MALF formal text and MALF diagram edition must be written next as direct child specifications of this plan.


## 12. Final Freeze

The formal reconstruction decisions frozen by this document are:

1. the new system is `lifespan-0.01 - structure - filter`
2. the formal mainline is `data -> malf -> alpha -> position -> portfolio_plan -> trade -> system`
3. old `raw/base` may be selectively reused
4. old `malf` and all its downstream semantic ledgers must not be reused as new-system truth
5. the first-edition stack is `Python + DuckDB + Arrow`
6. DuckDB manages ledgers
7. Arrow manages batch exchange
8. MALF is the only formal market structure truth layer
9. MALF preserves `reborn`
10. alpha is rebuilt as five independent PAS trigger ledgers plus one `alpha_signal` summary ledger
11. `position` reads only `alpha_signal`


This document is the formal master baseline for the new reconstruction.
