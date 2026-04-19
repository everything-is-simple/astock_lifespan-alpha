"""Stage-four position evaluation engine."""

from __future__ import annotations

from dataclasses import dataclass

from astock_lifespan_alpha.position.contracts import (
    CandidateStatus,
    CapacityStatus,
    PositionActionDecision,
    PositionInputRow,
)


STAGE_FOUR_POLICY_ID = "stage4_minimal_v1"


@dataclass(frozen=True)
class CandidateAuditRow:
    """Materialized candidate audit row."""

    candidate_nk: str
    signal_nk: str
    symbol: str
    signal_date: object
    trigger_type: str
    formal_signal_status: str
    candidate_status: str
    blocked_reason_code: str | None
    source_trigger_event_nk: str
    wave_id: str
    direction: str
    new_count: int
    no_new_span: int
    life_state: str
    update_rank: float
    stagnation_rank: float
    wave_position_zone: str
    reference_trade_date: object | None
    reference_price: float | None


@dataclass(frozen=True)
class CapacitySnapshotRow:
    """Materialized capacity row."""

    capacity_nk: str
    candidate_nk: str
    symbol: str
    signal_date: object
    policy_id: str
    capacity_status: str
    capacity_ceiling_weight: float
    reference_trade_date: object | None
    reference_price: float | None


@dataclass(frozen=True)
class SizingSnapshotRow:
    """Materialized sizing row."""

    sizing_nk: str
    candidate_nk: str
    symbol: str
    signal_date: object
    policy_id: str
    position_action_decision: str
    requested_weight: float
    final_allowed_position_weight: float
    required_reduction_weight: float
    candidate_status: str
    reference_trade_date: object | None
    reference_price: float | None


@dataclass(frozen=True)
class PositionEvaluationResult:
    """Grouped materialization results for one symbol."""

    candidates: list[CandidateAuditRow]
    capacities: list[CapacitySnapshotRow]
    sizings: list[SizingSnapshotRow]


def evaluate_position_rows(rows: list[PositionInputRow]) -> PositionEvaluationResult:
    """Evaluate joined alpha_signal rows into minimal position ledgers."""

    candidates: list[CandidateAuditRow] = []
    capacities: list[CapacitySnapshotRow] = []
    sizings: list[SizingSnapshotRow] = []

    for row in rows:
        candidate_status, blocked_reason_code = _derive_candidate_status(row)
        requested_weight = _derive_requested_weight(row)
        final_allowed_weight = requested_weight if candidate_status == CandidateStatus.ADMITTED.value else 0.0
        candidate_nk = row.signal_nk
        capacities.append(
            CapacitySnapshotRow(
                capacity_nk=f"{candidate_nk}:capacity",
                candidate_nk=candidate_nk,
                symbol=row.symbol,
                signal_date=row.signal_date,
                policy_id=STAGE_FOUR_POLICY_ID,
                capacity_status=(
                    CapacityStatus.ENABLED.value if final_allowed_weight > 0 else CapacityStatus.BLOCKED.value
                ),
                capacity_ceiling_weight=requested_weight,
                reference_trade_date=row.reference_trade_date,
                reference_price=row.reference_price,
            )
        )
        sizings.append(
            SizingSnapshotRow(
                sizing_nk=f"{candidate_nk}:sizing",
                candidate_nk=candidate_nk,
                symbol=row.symbol,
                signal_date=row.signal_date,
                policy_id=STAGE_FOUR_POLICY_ID,
                position_action_decision=(
                    PositionActionDecision.OPEN.value
                    if final_allowed_weight > 0
                    else PositionActionDecision.BLOCKED.value
                ),
                requested_weight=requested_weight,
                final_allowed_position_weight=final_allowed_weight,
                required_reduction_weight=0.0,
                candidate_status=candidate_status,
                reference_trade_date=row.reference_trade_date,
                reference_price=row.reference_price,
            )
        )
        candidates.append(
            CandidateAuditRow(
                candidate_nk=candidate_nk,
                signal_nk=row.signal_nk,
                symbol=row.symbol,
                signal_date=row.signal_date,
                trigger_type=row.trigger_type,
                formal_signal_status=row.formal_signal_status,
                candidate_status=candidate_status,
                blocked_reason_code=blocked_reason_code,
                source_trigger_event_nk=row.source_trigger_event_nk,
                wave_id=row.wave_id,
                direction=row.direction,
                new_count=row.new_count,
                no_new_span=row.no_new_span,
                life_state=row.life_state,
                update_rank=row.update_rank,
                stagnation_rank=row.stagnation_rank,
                wave_position_zone=row.wave_position_zone,
                reference_trade_date=row.reference_trade_date,
                reference_price=row.reference_price,
            )
        )

    return PositionEvaluationResult(candidates=candidates, capacities=capacities, sizings=sizings)


def _derive_candidate_status(row: PositionInputRow) -> tuple[str, str | None]:
    if row.reference_trade_date is None or row.reference_price is None:
        return CandidateStatus.BLOCKED.value, "missing_reference_price"
    if row.direction != "up":
        return CandidateStatus.BLOCKED.value, "direction_not_long"
    if row.formal_signal_status != "confirmed":
        return CandidateStatus.BLOCKED.value, "signal_not_confirmed"
    if row.wave_position_zone == "weak_stagnation":
        return CandidateStatus.BLOCKED.value, "weak_wave_position"
    return CandidateStatus.ADMITTED.value, None


def _derive_requested_weight(row: PositionInputRow) -> float:
    base_weight = {
        "early_progress": 0.12,
        "mature_progress": 0.10,
        "mature_stagnation": 0.06,
        "weak_stagnation": 0.0,
    }.get(row.wave_position_zone, 0.04)
    if row.update_rank < row.stagnation_rank:
        return round(base_weight * 0.8, 4)
    return round(base_weight, 4)
