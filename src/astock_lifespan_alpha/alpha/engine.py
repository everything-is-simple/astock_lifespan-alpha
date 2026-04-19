"""Formal alpha trigger and aggregation engine for stage three."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from statistics import mean

from astock_lifespan_alpha.alpha.contracts import AlphaInputRow, FormalSignalStatus, TriggerType


@dataclass(frozen=True)
class TriggerEventRow:
    event_nk: str
    symbol: str
    signal_date: date
    trigger_type: str
    formal_signal_status: str
    source_bar_dt: date
    wave_id: str
    direction: str
    new_count: int
    no_new_span: int
    life_state: str
    update_rank: float
    stagnation_rank: float
    wave_position_zone: str


@dataclass(frozen=True)
class TriggerProfileRow:
    profile_nk: str
    symbol: str
    trigger_type: str
    formal_signal_status: str
    event_count: int
    latest_signal_date: date
    avg_update_rank: float
    avg_stagnation_rank: float


@dataclass(frozen=True)
class AlphaSignalRow:
    signal_nk: str
    symbol: str
    signal_date: date
    trigger_type: str
    formal_signal_status: str
    source_trigger_db: str
    source_trigger_event_nk: str
    wave_id: str
    direction: str
    new_count: int
    no_new_span: int
    life_state: str
    update_rank: float
    stagnation_rank: float
    wave_position_zone: str


@dataclass(frozen=True)
class TriggerEvaluationResult:
    events: list[TriggerEventRow]
    profiles: list[TriggerProfileRow]


def evaluate_trigger_rows(*, trigger_type: TriggerType, rows: list[AlphaInputRow]) -> TriggerEvaluationResult:
    """Evaluate one trigger against ordered alpha input rows."""

    events: list[TriggerEventRow] = []
    previous_row: AlphaInputRow | None = None
    for current_row in rows:
        if previous_row is None:
            previous_row = current_row
            continue
        status = _evaluate_status(trigger_type=trigger_type, previous_row=previous_row, current_row=current_row)
        if status is None:
            previous_row = current_row
            continue
        events.append(
            TriggerEventRow(
                event_nk=f"{current_row.symbol}:{trigger_type.value}:{current_row.signal_date.isoformat()}:{current_row.wave_id}",
                symbol=current_row.symbol,
                signal_date=current_row.signal_date,
                trigger_type=trigger_type.value,
                formal_signal_status=status.value,
                source_bar_dt=current_row.signal_date,
                wave_id=current_row.wave_id,
                direction=current_row.direction,
                new_count=current_row.new_count,
                no_new_span=current_row.no_new_span,
                life_state=current_row.life_state,
                update_rank=current_row.update_rank,
                stagnation_rank=current_row.stagnation_rank,
                wave_position_zone=current_row.wave_position_zone,
            )
        )
        previous_row = current_row
    return TriggerEvaluationResult(events=events, profiles=_build_profiles(trigger_type=trigger_type, events=events))


def build_alpha_signal_rows(*, trigger_events: dict[str, list[TriggerEventRow]]) -> list[AlphaSignalRow]:
    """Build alpha_signal rows from trigger event collections."""

    signal_rows: list[AlphaSignalRow] = []
    for source_trigger_db, events in trigger_events.items():
        for event in events:
            signal_rows.append(
                AlphaSignalRow(
                    signal_nk=f"{event.trigger_type}:{event.event_nk}",
                    symbol=event.symbol,
                    signal_date=event.signal_date,
                    trigger_type=event.trigger_type,
                    formal_signal_status=event.formal_signal_status,
                    source_trigger_db=source_trigger_db,
                    source_trigger_event_nk=event.event_nk,
                    wave_id=event.wave_id,
                    direction=event.direction,
                    new_count=event.new_count,
                    no_new_span=event.no_new_span,
                    life_state=event.life_state,
                    update_rank=event.update_rank,
                    stagnation_rank=event.stagnation_rank,
                    wave_position_zone=event.wave_position_zone,
                )
            )
    return sorted(signal_rows, key=lambda row: (row.symbol, row.signal_date, row.trigger_type))


def _build_profiles(*, trigger_type: TriggerType, events: list[TriggerEventRow]) -> list[TriggerProfileRow]:
    grouped: dict[tuple[str, str], list[TriggerEventRow]] = {}
    for event in events:
        grouped.setdefault((event.symbol, event.formal_signal_status), []).append(event)
    profiles: list[TriggerProfileRow] = []
    for (symbol, status), grouped_events in grouped.items():
        profiles.append(
            TriggerProfileRow(
                profile_nk=f"{symbol}:{trigger_type.value}:{status}",
                symbol=symbol,
                trigger_type=trigger_type.value,
                formal_signal_status=status,
                event_count=len(grouped_events),
                latest_signal_date=max(event.signal_date for event in grouped_events),
                avg_update_rank=round(mean(event.update_rank for event in grouped_events), 2),
                avg_stagnation_rank=round(mean(event.stagnation_rank for event in grouped_events), 2),
            )
        )
    return profiles


def _evaluate_status(
    *,
    trigger_type: TriggerType,
    previous_row: AlphaInputRow,
    current_row: AlphaInputRow,
) -> FormalSignalStatus | None:
    if trigger_type is TriggerType.BOF:
        if (
            current_row.direction == "up"
            and current_row.life_state in {"alive", "reborn"}
            and current_row.wave_position_zone in {"early_progress", "mature_progress"}
            and current_row.high > previous_row.high
        ):
            if current_row.close >= previous_row.high:
                return FormalSignalStatus.CONFIRMED
            return FormalSignalStatus.CANDIDATE
        return None

    if trigger_type is TriggerType.TST:
        if (
            current_row.direction == "up"
            and current_row.no_new_span >= 1
            and current_row.wave_position_zone != "weak_stagnation"
            and current_row.low <= previous_row.high
        ):
            if current_row.close >= previous_row.high and current_row.close >= current_row.open:
                return FormalSignalStatus.CONFIRMED
            return FormalSignalStatus.CANDIDATE
        return None

    if trigger_type is TriggerType.PB:
        if (
            current_row.direction == "up"
            and current_row.life_state == "alive"
            and current_row.no_new_span >= 1
            and current_row.wave_position_zone in {"mature_progress", "mature_stagnation"}
            and current_row.close < previous_row.close
        ):
            if current_row.low >= previous_row.low:
                return FormalSignalStatus.CONFIRMED
            return FormalSignalStatus.CANDIDATE
        return None

    if trigger_type is TriggerType.CPB:
        if (
            current_row.direction == "up"
            and current_row.life_state == "alive"
            and current_row.no_new_span >= 1
            and current_row.wave_position_zone in {"mature_progress", "mature_stagnation"}
            and previous_row.close < previous_row.open
            and current_row.close >= current_row.open
        ):
            if current_row.close > previous_row.close:
                return FormalSignalStatus.CONFIRMED
            return FormalSignalStatus.CANDIDATE
        return None

    if trigger_type is TriggerType.BPB:
        if (
            current_row.direction == "down"
            and current_row.life_state in {"alive", "reborn"}
            and current_row.no_new_span >= 1
            and current_row.wave_position_zone in {"mature_progress", "mature_stagnation", "weak_stagnation"}
            and current_row.close < previous_row.close
        ):
            if current_row.low < previous_row.low:
                return FormalSignalStatus.CONFIRMED
            return FormalSignalStatus.CANDIDATE
        return None

    raise ValueError(f"Unsupported trigger type: {trigger_type}")
