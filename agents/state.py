"""Graph state for the RevOps goal-management system.

The state object is the contract between deterministic nodes and agent nodes.
Everything an agent produces lands here as typed data, never as free prose, so
the reconciliation and reporting steps downstream stay deterministic.
"""

from __future__ import annotations

import operator
from datetime import date, datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field

Severity = Literal["INFO", "WATCH", "AT_RISK", "CRITICAL"]
Department = Literal["MARKETING", "SALES", "FORECAST", "CS", "FINANCE"]


class Evidence(BaseModel):
    """Lineage for a single claim.

    A finding without evidence never reaches a report. The freshness timestamp
    is what lets the Data Quality agent downgrade a claim built on stale data
    instead of silently publishing it.
    """

    model_refs: list[str] = Field(
        default_factory=list,
        description="dbt models the claim was computed from, e.g. mart_goal_attainment.",
    )
    goal_ids: list[str] = Field(default_factory=list)
    row_count: int | None = None
    query_hash: str | None = None
    freshness_at: datetime | None = Field(
        default=None,
        description="Max loaded_at across every source touched by this claim.",
    )


class Finding(BaseModel):
    """One assertion made by one agent about one goal."""

    agent_name: str
    department: Department | None = None
    goal_id: str | None = None
    severity: Severity
    claim: str = Field(description="One sentence. No hedging, no preamble.")
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: Evidence
    proposed_action: str | None = None


class GoalNode(BaseModel):
    """A row of mart_goal_attainment, as the graph sees it."""

    goal_id: str
    organization_id: str
    parent_goal_id: str | None
    level: str
    owner_type: str
    owner_id: str
    metric_key: str
    fiscal_period_id: str
    target_value: float
    actual_value: float | None
    attainment_pct: float | None
    expected_attainment_pct: float | None
    pace_ratio: float | None
    relation_type: Literal["SUM", "DERIVED", "GUARDRAIL"]
    pacing_status: str
    source_status: Literal["COMPUTED", "NO_SOURCE"]
    rollup_status: str | None = None
    rollup_variance: float | None = None


class Reconciliation(BaseModel):
    """Deterministic output — computed in code, never by a model."""

    parent_goal_id: str
    fiscal_period_id: str
    sum_of_children: float | None
    parent_target: float
    variance: float | None
    is_balanced: bool
    relation_types_present: list[str]


class DataQualityVerdict(BaseModel):
    """The veto. If inputs are stale, findings publish as provisional."""

    is_fresh: bool
    max_staleness_hours: float | None
    failed_tests: list[str] = Field(default_factory=list)
    unmeasurable_metrics: list[str] = Field(default_factory=list)
    verdict: Literal["OK", "DEGRADED", "BLOCKED"]
    reason: str


class RevOpsState(BaseModel):
    """Top-level graph state.

    ``findings`` uses an additive reducer so the five department agents can be
    dispatched in parallel with Send and write into the same list without
    clobbering each other.
    """

    # --- inputs, set once at invocation ---
    organization_id: str = Field(description="Tenant scope. Every query is bound to this.")
    fiscal_period_id: str
    as_of_date: date

    # --- deterministic nodes populate these ---
    goals: list[GoalNode] = Field(default_factory=list)
    reconciliations: list[Reconciliation] = Field(default_factory=list)
    data_quality: DataQualityVerdict | None = None

    # --- agent nodes append here (parallel-safe) ---
    findings: Annotated[list[Finding], operator.add] = Field(default_factory=list)

    # --- terminal output ---
    narrative: str | None = None
    requires_approval: list[Finding] = Field(default_factory=list)

    run_id: str | None = None
