"""Pure semantic MALF engine for stage-two reconstruction."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from astock_lifespan_alpha.malf.contracts import LifeState, OhlcBar, Timeframe, WaveDirection, WavePositionZone


@dataclass(frozen=True)
class PivotRow:
    pivot_nk: str
    symbol: str
    timeframe: str
    wave_id: str
    bar_dt: datetime
    pivot_type: str
    price: float


@dataclass(frozen=True)
class WaveRow:
    wave_id: str
    symbol: str
    timeframe: str
    direction: str
    start_bar_dt: datetime
    end_bar_dt: datetime
    guard_bar_dt: datetime
    guard_price: float
    extreme_price: float
    new_count: int
    no_new_span: int
    life_state: str


@dataclass(frozen=True)
class SnapshotRow:
    snapshot_nk: str
    symbol: str
    timeframe: str
    bar_dt: datetime
    wave_id: str
    direction: str
    guard_price: float
    extreme_price: float
    new_count: int
    no_new_span: int
    life_state: str
    update_rank: float
    stagnation_rank: float
    wave_position_zone: str


@dataclass(frozen=True)
class ProfileRow:
    profile_nk: str
    symbol: str
    timeframe: str
    direction: str
    wave_id: str
    sample_size: int
    new_count: int
    no_new_span: int
    update_rank: float
    stagnation_rank: float
    wave_position_zone: str


@dataclass
class _WaveState:
    wave_index: int
    direction: WaveDirection
    start_bar_dt: datetime
    guard_bar_dt: datetime
    guard_price: float
    extreme_price: float
    new_count: int
    no_new_span: int
    life_state: LifeState


@dataclass(frozen=True)
class EngineResult:
    pivots: list[PivotRow]
    waves: list[WaveRow]
    state_snapshots: list[SnapshotRow]
    wave_scale_snapshots: list[SnapshotRow]
    wave_scale_profiles: list[ProfileRow]


def run_malf_engine(*, symbol: str, timeframe: Timeframe, bars: Iterable[OhlcBar]) -> EngineResult:
    """Materialize MALF ledgers for one symbol and one timeframe."""

    ordered_bars = sorted(list(bars), key=lambda bar: bar.bar_dt)
    if not ordered_bars:
        return EngineResult(pivots=[], waves=[], state_snapshots=[], wave_scale_snapshots=[], wave_scale_profiles=[])

    state = _initialize_state(symbol=symbol, timeframe=timeframe, bars=ordered_bars)
    pivots: list[PivotRow] = []
    snapshots: list[SnapshotRow] = []
    waves: list[WaveRow] = []
    previous_bar = ordered_bars[0]

    snapshots.append(
        SnapshotRow(
            snapshot_nk=f"{symbol}:{timeframe.value}:{previous_bar.bar_dt.isoformat()}",
            symbol=symbol,
            timeframe=timeframe.value,
            bar_dt=previous_bar.bar_dt,
            wave_id=_wave_id(symbol, timeframe, state.wave_index),
            direction=state.direction.value,
            guard_price=state.guard_price,
            extreme_price=state.extreme_price,
            new_count=state.new_count,
            no_new_span=state.no_new_span,
            life_state=state.life_state.value,
            update_rank=0.0,
            stagnation_rank=0.0,
            wave_position_zone=WavePositionZone.EARLY_PROGRESS.value,
        )
    )

    for bar in ordered_bars[1:]:
        event_pivots, finished_wave, next_state = _transition_state(
            symbol=symbol,
            timeframe=timeframe,
            current_state=state,
            previous_bar=previous_bar,
            current_bar=bar,
        )
        pivots.extend(event_pivots)
        if finished_wave is not None:
            waves.append(finished_wave)
        state = next_state
        snapshots.append(
            SnapshotRow(
                snapshot_nk=f"{symbol}:{timeframe.value}:{bar.bar_dt.isoformat()}",
                symbol=symbol,
                timeframe=timeframe.value,
                bar_dt=bar.bar_dt,
                wave_id=_wave_id(symbol, timeframe, state.wave_index),
                direction=state.direction.value,
                guard_price=state.guard_price,
                extreme_price=state.extreme_price,
                new_count=state.new_count,
                no_new_span=state.no_new_span,
                life_state=state.life_state.value,
                update_rank=0.0,
                stagnation_rank=0.0,
                wave_position_zone=WavePositionZone.EARLY_PROGRESS.value,
            )
        )
        previous_bar = bar

    waves.append(
        WaveRow(
            wave_id=_wave_id(symbol, timeframe, state.wave_index),
            symbol=symbol,
            timeframe=timeframe.value,
            direction=state.direction.value,
            start_bar_dt=state.start_bar_dt,
            end_bar_dt=ordered_bars[-1].bar_dt,
            guard_bar_dt=state.guard_bar_dt,
            guard_price=state.guard_price,
            extreme_price=state.extreme_price,
            new_count=state.new_count,
            no_new_span=state.no_new_span,
            life_state=state.life_state.value,
        )
    )

    ranked_snapshots = _rank_snapshots(snapshots=snapshots, waves=waves)
    profiles = _build_profiles(waves=waves)
    return EngineResult(
        pivots=pivots,
        waves=waves,
        state_snapshots=ranked_snapshots,
        wave_scale_snapshots=ranked_snapshots,
        wave_scale_profiles=profiles,
    )


def _initialize_state(symbol: str, timeframe: Timeframe, bars: list[OhlcBar]) -> _WaveState:
    first_bar = bars[0]
    if len(bars) == 1:
        direction = WaveDirection.UP
    else:
        direction = WaveDirection.UP if bars[1].close >= first_bar.close else WaveDirection.DOWN
    if direction is WaveDirection.UP:
        guard_price = first_bar.low
        extreme_price = first_bar.high
    else:
        guard_price = first_bar.high
        extreme_price = first_bar.low
    return _WaveState(
        wave_index=1,
        direction=direction,
        start_bar_dt=first_bar.bar_dt,
        guard_bar_dt=first_bar.bar_dt,
        guard_price=guard_price,
        extreme_price=extreme_price,
        new_count=0,
        no_new_span=0,
        life_state=LifeState.ALIVE,
    )


def _transition_state(
    *,
    symbol: str,
    timeframe: Timeframe,
    current_state: _WaveState,
    previous_bar: OhlcBar,
    current_bar: OhlcBar,
) -> tuple[list[PivotRow], WaveRow | None, _WaveState]:
    pivots: list[PivotRow] = []
    wave_id = _wave_id(symbol, timeframe, current_state.wave_index)
    if current_state.direction is WaveDirection.UP:
        if current_bar.low < current_state.guard_price:
            finished_wave = WaveRow(
                wave_id=wave_id,
                symbol=symbol,
                timeframe=timeframe.value,
                direction=current_state.direction.value,
                start_bar_dt=current_state.start_bar_dt,
                end_bar_dt=current_bar.bar_dt,
                guard_bar_dt=current_state.guard_bar_dt,
                guard_price=current_state.guard_price,
                extreme_price=current_state.extreme_price,
                new_count=current_state.new_count,
                no_new_span=current_state.no_new_span,
                life_state=LifeState.BROKEN.value,
            )
            pivots.append(_pivot(symbol, timeframe, wave_id, current_bar.bar_dt, "break_down", current_bar.low))
            next_state = _WaveState(
                wave_index=current_state.wave_index + 1,
                direction=WaveDirection.DOWN,
                start_bar_dt=current_bar.bar_dt,
                guard_bar_dt=current_bar.bar_dt,
                guard_price=current_bar.high,
                extreme_price=current_bar.low,
                new_count=0,
                no_new_span=0,
                life_state=LifeState.REBORN,
            )
            return pivots, finished_wave, next_state

        next_state = _WaveState(**current_state.__dict__)
        if current_bar.high > current_state.extreme_price:
            next_state.extreme_price = current_bar.high
            next_state.new_count += 1
            next_state.no_new_span = 0
            if next_state.life_state is LifeState.REBORN:
                next_state.life_state = LifeState.ALIVE
            pivots.append(_pivot(symbol, timeframe, wave_id, current_bar.bar_dt, "HH", current_bar.high))
        else:
            next_state.no_new_span += 1
        if current_bar.low > next_state.guard_price:
            next_state.guard_price = current_bar.low
            next_state.guard_bar_dt = current_bar.bar_dt
            pivots.append(_pivot(symbol, timeframe, wave_id, current_bar.bar_dt, "HL", current_bar.low))
        return pivots, None, next_state

    if current_bar.high > current_state.guard_price:
        finished_wave = WaveRow(
            wave_id=wave_id,
            symbol=symbol,
            timeframe=timeframe.value,
            direction=current_state.direction.value,
            start_bar_dt=current_state.start_bar_dt,
            end_bar_dt=current_bar.bar_dt,
            guard_bar_dt=current_state.guard_bar_dt,
            guard_price=current_state.guard_price,
            extreme_price=current_state.extreme_price,
            new_count=current_state.new_count,
            no_new_span=current_state.no_new_span,
            life_state=LifeState.BROKEN.value,
        )
        pivots.append(_pivot(symbol, timeframe, wave_id, current_bar.bar_dt, "break_up", current_bar.high))
        next_state = _WaveState(
            wave_index=current_state.wave_index + 1,
            direction=WaveDirection.UP,
            start_bar_dt=current_bar.bar_dt,
            guard_bar_dt=current_bar.bar_dt,
            guard_price=current_bar.low,
            extreme_price=current_bar.high,
            new_count=0,
            no_new_span=0,
            life_state=LifeState.REBORN,
        )
        return pivots, finished_wave, next_state

    next_state = _WaveState(**current_state.__dict__)
    if current_bar.low < current_state.extreme_price:
        next_state.extreme_price = current_bar.low
        next_state.new_count += 1
        next_state.no_new_span = 0
        if next_state.life_state is LifeState.REBORN:
            next_state.life_state = LifeState.ALIVE
        pivots.append(_pivot(symbol, timeframe, wave_id, current_bar.bar_dt, "LL", current_bar.low))
    else:
        next_state.no_new_span += 1
    if current_bar.high < next_state.guard_price:
        next_state.guard_price = current_bar.high
        next_state.guard_bar_dt = current_bar.bar_dt
        pivots.append(_pivot(symbol, timeframe, wave_id, current_bar.bar_dt, "LH", current_bar.high))
    return pivots, None, next_state


def _pivot(symbol: str, timeframe: Timeframe, wave_id: str, bar_dt: datetime, pivot_type: str, price: float) -> PivotRow:
    return PivotRow(
        pivot_nk=f"{wave_id}:{pivot_type}:{bar_dt.isoformat()}",
        symbol=symbol,
        timeframe=timeframe.value,
        wave_id=wave_id,
        bar_dt=bar_dt,
        pivot_type=pivot_type,
        price=price,
    )


def _rank_snapshots(*, snapshots: list[SnapshotRow], waves: list[WaveRow]) -> list[SnapshotRow]:
    ranked_snapshots: list[SnapshotRow] = []
    for snapshot in snapshots:
        sample = [
            wave
            for wave in waves
            if wave.symbol == snapshot.symbol and wave.timeframe == snapshot.timeframe and wave.direction == snapshot.direction
        ]
        sample_size = max(1, len(sample))
        update_rank = _percentile(
            value=snapshot.new_count,
            sample=[wave.new_count for wave in sample] or [snapshot.new_count],
        )
        stagnation_rank = _percentile(
            value=snapshot.no_new_span,
            sample=[wave.no_new_span for wave in sample] or [snapshot.no_new_span],
        )
        zone = _classify_zone(
            update_rank=update_rank,
            stagnation_rank=stagnation_rank,
            life_state=LifeState(snapshot.life_state),
        )
        ranked_snapshots.append(
            SnapshotRow(
                snapshot_nk=snapshot.snapshot_nk,
                symbol=snapshot.symbol,
                timeframe=snapshot.timeframe,
                bar_dt=snapshot.bar_dt,
                wave_id=snapshot.wave_id,
                direction=snapshot.direction,
                guard_price=snapshot.guard_price,
                extreme_price=snapshot.extreme_price,
                new_count=snapshot.new_count,
                no_new_span=snapshot.no_new_span,
                life_state=snapshot.life_state,
                update_rank=update_rank,
                stagnation_rank=stagnation_rank,
                wave_position_zone=zone.value if sample_size else WavePositionZone.EARLY_PROGRESS.value,
            )
        )
    return ranked_snapshots


def _build_profiles(*, waves: list[WaveRow]) -> list[ProfileRow]:
    profiles: list[ProfileRow] = []
    for wave in waves:
        sample = [
            candidate
            for candidate in waves
            if candidate.symbol == wave.symbol
            and candidate.timeframe == wave.timeframe
            and candidate.direction == wave.direction
        ]
        update_rank = _percentile(value=wave.new_count, sample=[candidate.new_count for candidate in sample])
        stagnation_rank = _percentile(value=wave.no_new_span, sample=[candidate.no_new_span for candidate in sample])
        profiles.append(
            ProfileRow(
                profile_nk=f"{wave.wave_id}:profile",
                symbol=wave.symbol,
                timeframe=wave.timeframe,
                direction=wave.direction,
                wave_id=wave.wave_id,
                sample_size=len(sample),
                new_count=wave.new_count,
                no_new_span=wave.no_new_span,
                update_rank=update_rank,
                stagnation_rank=stagnation_rank,
                wave_position_zone=_classify_zone(
                    update_rank=update_rank,
                    stagnation_rank=stagnation_rank,
                    life_state=LifeState(wave.life_state),
                ).value,
            )
        )
    return profiles


def _percentile(*, value: int, sample: list[int]) -> float:
    if not sample:
        return 0.0
    less_or_equal = sum(1 for candidate in sample if candidate <= value)
    return round((less_or_equal / len(sample)) * 100.0, 2)


def _classify_zone(*, update_rank: float, stagnation_rank: float, life_state: LifeState) -> WavePositionZone:
    if life_state is LifeState.REBORN:
        return WavePositionZone.EARLY_PROGRESS
    if stagnation_rank < 50.0:
        if update_rank < 50.0:
            return WavePositionZone.EARLY_PROGRESS
        return WavePositionZone.MATURE_PROGRESS
    if update_rank >= 50.0:
        return WavePositionZone.MATURE_STAGNATION
    return WavePositionZone.WEAK_STAGNATION


def _wave_id(symbol: str, timeframe: Timeframe, wave_index: int) -> str:
    return f"{symbol}:{timeframe.value}:wave:{wave_index:04d}"
