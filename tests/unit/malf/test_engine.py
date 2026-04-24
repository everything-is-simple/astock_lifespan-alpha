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

    assert _project_snapshot_ranks(ranked) == _project_snapshot_ranks(
        _legacy_rank_snapshots(snapshots=snapshots, waves=waves)
    )
    assert _project_profile_ranks(profiles) == _project_profile_ranks(_legacy_build_profiles(waves=waves))


def test_run_malf_engine_keeps_reborn_until_prior_guard_candidate_gets_confirmed():
    bars = [
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 2), open=10.0, high=11.0, low=9.0, close=10.8),
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 3), open=10.8, high=12.5, low=10.0, close=12.0),
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 4), open=12.0, high=12.2, low=10.2, close=10.5),
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 5), open=10.5, high=10.7, low=8.8, close=9.1),
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 6), open=9.1, high=9.7, low=8.9, close=9.4),
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 7), open=9.4, high=9.5, low=8.4, close=8.6),
    ]

    result = engine.run_malf_engine(symbol="AAA", timeframe=Timeframe.DAY, bars=bars)
    snapshots = {row.bar_dt.date().isoformat(): row for row in result.state_snapshots}
    down_wave_snapshots = [row for row in result.state_snapshots if row.wave_id.endswith("0002")]
    down_wave_pivots = [row for row in result.pivots if row.wave_id.endswith("0002")]

    assert snapshots["2026-01-05"].life_state == "reborn"
    assert snapshots["2026-01-06"].life_state == "reborn"
    assert snapshots["2026-01-07"].life_state == "alive"
    assert snapshots["2026-01-06"].new_count == 0
    assert snapshots["2026-01-06"].no_new_span == 1
    assert snapshots["2026-01-07"].new_count == 1
    assert [row.life_state for row in down_wave_snapshots] == ["reborn", "reborn", "alive"]
    assert [row.pivot_type for row in down_wave_pivots] == ["LL", "LH"]


def test_run_malf_engine_confirms_guard_only_on_structure_resume():
    bars = [
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 2), open=10.0, high=11.0, low=9.0, close=10.8),
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 3), open=10.8, high=12.5, low=10.0, close=12.0),
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 4), open=12.0, high=12.3, low=10.2, close=11.0),
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 5), open=11.0, high=12.4, low=10.6, close=11.4),
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 6), open=11.4, high=13.0, low=11.0, close=12.8),
    ]

    result = engine.run_malf_engine(symbol="AAA", timeframe=Timeframe.DAY, bars=bars)
    snapshots = {row.bar_dt.date().isoformat(): row for row in result.state_snapshots}
    up_pivots = [row for row in result.pivots if row.wave_id.endswith("0001")]

    assert snapshots["2026-01-03"].guard_price == 9.0
    assert snapshots["2026-01-04"].guard_price == 9.0
    assert snapshots["2026-01-05"].guard_price == 9.0
    assert snapshots["2026-01-06"].guard_price == 10.6
    assert [(row.bar_dt.date().isoformat(), row.pivot_type, row.price) for row in up_pivots] == [
        ("2026-01-03", "HH", 12.5),
        ("2026-01-06", "HH", 13.0),
        ("2026-01-06", "HL", 10.6),
    ]


def test_up_break_starts_down_reborn_and_waits_for_ll_confirmation():
    bars = [
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 2), open=10.0, high=11.0, low=9.0, close=10.6),
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 3), open=10.6, high=12.5, low=10.0, close=12.0),
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 4), open=12.0, high=12.4, low=10.4, close=11.0),
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 5), open=11.0, high=10.5, low=8.8, close=9.0),
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 6), open=9.0, high=10.2, low=8.9, close=9.5),
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 7), open=9.5, high=9.9, low=8.7, close=8.8),
    ]

    result = engine.run_malf_engine(symbol="AAA", timeframe=Timeframe.DAY, bars=bars)
    snapshots = {row.bar_dt.date().isoformat(): row for row in result.state_snapshots}
    wave2_pivots = [row for row in result.pivots if row.wave_id.endswith("0002")]

    assert snapshots["2026-01-05"].direction == "down"
    assert snapshots["2026-01-05"].life_state == "reborn"
    assert snapshots["2026-01-05"].new_count == 0
    assert snapshots["2026-01-05"].no_new_span == 0
    assert snapshots["2026-01-06"].life_state == "reborn"
    assert snapshots["2026-01-06"].new_count == 0
    assert snapshots["2026-01-06"].no_new_span == 1
    assert snapshots["2026-01-07"].life_state == "alive"
    assert snapshots["2026-01-07"].new_count == 1
    assert snapshots["2026-01-07"].no_new_span == 0
    assert [(row.bar_dt.date().isoformat(), row.pivot_type) for row in wave2_pivots] == [
        ("2026-01-07", "LL"),
        ("2026-01-07", "LH"),
    ]


def test_down_break_starts_up_reborn_and_waits_for_hh_confirmation():
    bars = [
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 2), open=10.0, high=11.0, low=9.0, close=10.0),
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 3), open=10.0, high=10.5, low=8.5, close=8.8),
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 4), open=8.8, high=10.0, low=8.7, close=9.0),
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 5), open=9.0, high=11.2, low=9.2, close=10.9),
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 6), open=10.9, high=11.1, low=9.8, close=10.5),
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 7), open=10.5, high=11.15, low=10.1, close=10.8),
        OhlcBar(symbol="AAA", bar_dt=datetime(2026, 1, 8), open=10.8, high=11.8, low=10.6, close=11.6),
    ]

    result = engine.run_malf_engine(symbol="AAA", timeframe=Timeframe.DAY, bars=bars)
    snapshots = {row.bar_dt.date().isoformat(): row for row in result.state_snapshots}
    wave2_pivots = [row for row in result.pivots if row.wave_id.endswith("0002")]

    assert snapshots["2026-01-05"].direction == "up"
    assert snapshots["2026-01-05"].life_state == "reborn"
    assert snapshots["2026-01-05"].new_count == 0
    assert snapshots["2026-01-05"].no_new_span == 0
    assert snapshots["2026-01-06"].life_state == "reborn"
    assert snapshots["2026-01-06"].new_count == 0
    assert snapshots["2026-01-06"].no_new_span == 1
    assert snapshots["2026-01-07"].life_state == "reborn"
    assert snapshots["2026-01-07"].new_count == 0
    assert snapshots["2026-01-07"].no_new_span == 2
    assert snapshots["2026-01-08"].life_state == "alive"
    assert snapshots["2026-01-08"].new_count == 1
    assert snapshots["2026-01-08"].no_new_span == 0
    assert [(row.bar_dt.date().isoformat(), row.pivot_type) for row in wave2_pivots] == [
        ("2026-01-08", "HH"),
        ("2026-01-08", "HL"),
    ]


def test_rank_and_zone_classification_cover_four_quadrants():
    waves = [
        engine.WaveRow(
            wave_id="wave-up-low",
            symbol="AAA",
            timeframe="day",
            direction="up",
            start_bar_dt=datetime(2026, 1, 2),
            end_bar_dt=datetime(2026, 1, 3),
            guard_bar_dt=datetime(2026, 1, 2),
            guard_price=9.5,
            extreme_price=11.0,
            new_count=1,
            no_new_span=1,
            life_state="alive",
        ),
        engine.WaveRow(
            wave_id="wave-up-high",
            symbol="AAA",
            timeframe="day",
            direction="up",
            start_bar_dt=datetime(2026, 1, 4),
            end_bar_dt=datetime(2026, 1, 5),
            guard_bar_dt=datetime(2026, 1, 4),
            guard_price=10.0,
            extreme_price=12.0,
            new_count=5,
            no_new_span=2,
            life_state="alive",
        ),
        engine.WaveRow(
            wave_id="wave-up-stall",
            symbol="AAA",
            timeframe="day",
            direction="up",
            start_bar_dt=datetime(2026, 1, 6),
            end_bar_dt=datetime(2026, 1, 7),
            guard_bar_dt=datetime(2026, 1, 6),
            guard_price=10.2,
            extreme_price=12.2,
            new_count=4,
            no_new_span=6,
            life_state="alive",
        ),
        engine.WaveRow(
            wave_id="wave-up-weak",
            symbol="AAA",
            timeframe="day",
            direction="up",
            start_bar_dt=datetime(2026, 1, 8),
            end_bar_dt=datetime(2026, 1, 9),
            guard_bar_dt=datetime(2026, 1, 8),
            guard_price=10.4,
            extreme_price=12.3,
            new_count=2,
            no_new_span=7,
            life_state="alive",
        ),
        engine.WaveRow(
            wave_id="wave-up-anchor",
            symbol="AAA",
            timeframe="day",
            direction="up",
            start_bar_dt=datetime(2026, 1, 10),
            end_bar_dt=datetime(2026, 1, 11),
            guard_bar_dt=datetime(2026, 1, 10),
            guard_price=10.5,
            extreme_price=12.5,
            new_count=7,
            no_new_span=4,
            life_state="alive",
        ),
        engine.WaveRow(
            wave_id="wave-down-reborn",
            symbol="AAA",
            timeframe="day",
            direction="down",
            start_bar_dt=datetime(2026, 1, 12),
            end_bar_dt=datetime(2026, 1, 13),
            guard_bar_dt=datetime(2026, 1, 12),
            guard_price=12.6,
            extreme_price=9.8,
            new_count=0,
            no_new_span=1,
            life_state="reborn",
        ),
    ]
    snapshots = [
        _snapshot("snap-early", "wave-up-low", "up", datetime(2026, 1, 3), 1, 1, "alive"),
        _snapshot("snap-mature", "wave-up-high", "up", datetime(2026, 1, 5), 5, 2, "alive"),
        _snapshot("snap-mature-stall", "wave-up-stall", "up", datetime(2026, 1, 7), 4, 6, "alive"),
        _snapshot("snap-weak", "wave-up-weak", "up", datetime(2026, 1, 9), 2, 7, "alive"),
        _snapshot("snap-reborn", "wave-down-reborn", "down", datetime(2026, 1, 13), 0, 1, "reborn"),
    ]

    sample_pools = engine._build_wave_sample_pools(waves)
    ranked = engine._rank_snapshots(snapshots=snapshots, sample_pools=sample_pools)

    assert {
        row.snapshot_nk: row.wave_position_zone
        for row in ranked
    } == {
        "snap-early": "early_progress",
        "snap-mature": "mature_progress",
        "snap-mature-stall": "mature_stagnation",
        "snap-weak": "weak_stagnation",
        "snap-reborn": "early_progress",
    }
    assert {row.wave_position_zone for row in ranked} == {
        "early_progress",
        "mature_progress",
        "mature_stagnation",
        "weak_stagnation",
    }


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


def _snapshot(
    snapshot_nk: str,
    wave_id: str,
    direction: str,
    bar_dt: datetime,
    new_count: int,
    no_new_span: int,
    life_state: str,
) -> engine.SnapshotRow:
    return engine.SnapshotRow(
        snapshot_nk=snapshot_nk,
        symbol="AAA",
        timeframe="day",
        bar_dt=bar_dt,
        wave_id=wave_id,
        direction=direction,
        guard_price=9.5,
        extreme_price=11.0,
        new_count=new_count,
        no_new_span=no_new_span,
        life_state=life_state,
        update_rank=0.0,
        stagnation_rank=0.0,
        wave_position_zone="early_progress",
    )


def _project_snapshot_ranks(rows: list[engine.SnapshotRow]) -> list[tuple[str, float, float]]:
    return [
        (row.snapshot_nk, row.update_rank, row.stagnation_rank)
        for row in rows
    ]


def _project_profile_ranks(rows: list[engine.ProfileRow]) -> list[tuple[str, int, float, float]]:
    return [
        (row.profile_nk, row.sample_size, row.update_rank, row.stagnation_rank)
        for row in rows
    ]
