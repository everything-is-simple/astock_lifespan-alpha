from __future__ import annotations

from datetime import datetime

import pytest

from astock_lifespan_alpha.malf import engine
from astock_lifespan_alpha.malf.contracts import LifeState, OhlcBar, Timeframe


def test_run_malf_engine_rejects_duplicate_bar_dt():
    bars = [
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 2), open=10.0, high=11.0, low=9.5, close=10.8),
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 2), open=10.2, high=11.2, low=9.7, close=10.9),
    ]

    with pytest.raises(ValueError, match="unique bar_dt"):
        engine.run_malf_engine(symbol="AAA", timeframe=Timeframe.DAY, bars=bars)


def test_cached_rank_and_profile_computation_matches_legacy_semantics():
    waves = [
        engine.WaveRow(
            wave_id="wave-1",
            symbol="AAA",
            timeframe="day",
            direction="up",
            start_bar_dt=datetime(2026, 1, 2),
            end_bar_dt=datetime(2026, 1, 3),
            guard_bar_dt=datetime(2026, 1, 2),
            guard_price=9.5,
            extreme_price=11.0,
            new_count=1,
            no_new_span=3,
            life_state="alive",
        ),
        engine.WaveRow(
            wave_id="wave-2",
            symbol="AAA",
            timeframe="day",
            direction="up",
            start_bar_dt=datetime(2026, 1, 4),
            end_bar_dt=datetime(2026, 1, 5),
            guard_bar_dt=datetime(2026, 1, 4),
            guard_price=10.0,
            extreme_price=12.0,
            new_count=4,
            no_new_span=1,
            life_state="alive",
        ),
        engine.WaveRow(
            wave_id="wave-3",
            symbol="AAA",
            timeframe="day",
            direction="down",
            start_bar_dt=datetime(2026, 1, 6),
            end_bar_dt=datetime(2026, 1, 7),
            guard_bar_dt=datetime(2026, 1, 6),
            guard_price=12.2,
            extreme_price=9.8,
            new_count=2,
            no_new_span=2,
            life_state="reborn",
        ),
    ]
    snapshots = [
        engine.SnapshotRow(
            snapshot_nk="snap-1",
            symbol="AAA",
            timeframe="day",
            bar_dt=datetime(2026, 1, 3),
            wave_id="wave-1",
            direction="up",
            guard_price=9.5,
            extreme_price=11.0,
            new_count=1,
            no_new_span=3,
            life_state="alive",
            update_rank=0.0,
            stagnation_rank=0.0,
            wave_position_zone="early_progress",
        ),
        engine.SnapshotRow(
            snapshot_nk="snap-2",
            symbol="AAA",
            timeframe="day",
            bar_dt=datetime(2026, 1, 5),
            wave_id="wave-2",
            direction="up",
            guard_price=10.0,
            extreme_price=12.0,
            new_count=4,
            no_new_span=1,
            life_state="alive",
            update_rank=0.0,
            stagnation_rank=0.0,
            wave_position_zone="early_progress",
        ),
        engine.SnapshotRow(
            snapshot_nk="snap-3",
            symbol="AAA",
            timeframe="day",
            bar_dt=datetime(2026, 1, 7),
            wave_id="wave-3",
            direction="down",
            guard_price=12.2,
            extreme_price=9.8,
            new_count=2,
            no_new_span=2,
            life_state="reborn",
            update_rank=0.0,
            stagnation_rank=0.0,
            wave_position_zone="early_progress",
        ),
    ]

    sample_pools = engine._build_wave_sample_pools(waves)
    ranked = engine._rank_snapshots(snapshots=snapshots, sample_pools=sample_pools)
    profiles = engine._build_profiles(waves=waves, sample_pools=sample_pools)

    assert _project_snapshots(ranked) == _project_snapshots(_legacy_rank_snapshots(snapshots=snapshots, waves=waves))
    assert _project_profiles(profiles) == _project_profiles(_legacy_build_profiles(waves=waves))


def _legacy_rank_snapshots(
    *,
    snapshots: list[engine.SnapshotRow],
    waves: list[engine.WaveRow],
) -> list[engine.SnapshotRow]:
    ranked_snapshots: list[engine.SnapshotRow] = []
    for snapshot in snapshots:
        sample = [
            wave
            for wave in waves
            if wave.symbol == snapshot.symbol and wave.timeframe == snapshot.timeframe and wave.direction == snapshot.direction
        ]
        update_rank = engine._percentile(
            value=snapshot.new_count,
            sample=[wave.new_count for wave in sample] or [snapshot.new_count],
        )
        stagnation_rank = engine._percentile(
            value=snapshot.no_new_span,
            sample=[wave.no_new_span for wave in sample] or [snapshot.no_new_span],
        )
        ranked_snapshots.append(
            engine.SnapshotRow(
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
                wave_position_zone=engine._classify_zone(
                    update_rank=update_rank,
                    stagnation_rank=stagnation_rank,
                    life_state=LifeState(snapshot.life_state),
                ).value,
            )
        )
    return ranked_snapshots


def _legacy_build_profiles(*, waves: list[engine.WaveRow]) -> list[engine.ProfileRow]:
    profiles: list[engine.ProfileRow] = []
    for wave in waves:
        sample = [
            candidate
            for candidate in waves
            if candidate.symbol == wave.symbol
            and candidate.timeframe == wave.timeframe
            and candidate.direction == wave.direction
        ]
        update_rank = engine._percentile(value=wave.new_count, sample=[candidate.new_count for candidate in sample])
        stagnation_rank = engine._percentile(value=wave.no_new_span, sample=[candidate.no_new_span for candidate in sample])
        profiles.append(
            engine.ProfileRow(
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
                wave_position_zone=engine._classify_zone(
                    update_rank=update_rank,
                    stagnation_rank=stagnation_rank,
                    life_state=LifeState(wave.life_state),
                ).value,
            )
        )
    return profiles


def _project_snapshots(rows: list[engine.SnapshotRow]) -> list[tuple[str, float, float, str]]:
    return [
        (row.snapshot_nk, row.update_rank, row.stagnation_rank, row.wave_position_zone)
        for row in rows
    ]


def _project_profiles(rows: list[engine.ProfileRow]) -> list[tuple[str, int, float, float, str]]:
    return [
        (row.profile_nk, row.sample_size, row.update_rank, row.stagnation_rank, row.wave_position_zone)
        for row in rows
    ]
