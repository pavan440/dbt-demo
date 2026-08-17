"""The RevOps goal-management graph.

Shape
-----
    load_goals ──▶ reconcile ──▶ data_quality ──┬─▶ [demand]     ─┐
      (code)        (code)          (code)      ├─▶ [pipeline]   │
                                                ├─▶ [forecast]   ├─▶ narrate ─▶ approval_gate
                                                ├─▶ [retention]  │    (agent)      (interrupt)
                                                └─▶ [efficiency] ─┘

Only the bracketed nodes are agents. Loading the goal tree, reconciling parents
against children, and judging data freshness are deterministic operations — they
run as plain Python so they are reproducible, cheap, and testable. Agents are
used where the work is genuinely interpretive: explaining variance and proposing
interventions.
"""

from __future__ import annotations

import os
from typing import Literal

from langchain_anthropic import ChatAnthropic
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Send, interrupt
from langgraph.prebuilt import create_react_agent

from agents.state import (
    DataQualityVerdict,
    Department,
    Evidence,
    Finding,
    GoalNode,
    Reconciliation,
    RevOpsState,
)
from agents.tools.snowflake import query_warehouse, set_tenant

MODEL = os.environ.get("REVOPS_MODEL", "claude-opus-5")

# Department agents map to branches of the goal tree. Each owns its metrics and
# is told, explicitly, not to comment outside its own branch — otherwise five
# agents produce five overlapping opinions on the same company number and the
# orchestrator has no basis on which to adjudicate.
DEPARTMENT_BRIEFS: dict[Department, str] = {
    "MARKETING": (
        "You own the marketing branch: sourced pipeline against target, channel mix, "
        "cost per lead and CAC drift, and MQL quality."
    ),
    "SALES": (
        "You own the sales branch: pipeline coverage, stage conversion, sales velocity, "
        "deal-level risk, and rep and team quota attainment."
    ),
    "FORECAST": (
        "You own the risk-adjusted projection against the company goal: commit versus "
        "actual, and forecast accuracy over time."
    ),
    "CS": (
        "You own the customer success branch: net and gross retention pace, churn risk, "
        "expansion pipeline, and renewal exposure by quarter."
    ),
    "FINANCE": (
        "You own the finance guardrails: CAC payback, LTV to CAC, margin, and spend "
        "against plan. Guardrails constrain the plan; they do not sum into it."
    ),
}

SHARED_RULES = """
You are a department agent in a multi-tenant RevOps goal system.

Rules that are not negotiable:
- Every number you cite must come from a query_warehouse call in this run. Never
  recall a figure from earlier context or estimate one.
- A goal whose source_status is NO_SOURCE is a data gap, not a business problem.
  Report it as unmeasurable. Never call it at risk.
- GUARDRAIL goals constrain the plan rather than contributing to it. Do not
  include them in any rollup arithmetic.
- Comment only on your own branch. If you notice something in another
  department's branch, say so in one line and move on.
- Report outcomes faithfully. If the data does not support a claim, say that
  plainly rather than softening it.

Return findings as structured data, not prose. Lead with the outcome.
"""


def _llm() -> ChatAnthropic:
    return ChatAnthropic(model=MODEL, max_tokens=8000)


# ---------------------------------------------------------------- deterministic


def load_goals(state: RevOpsState) -> dict:
    """Pull the tenant's goal tree for the period. Pure query, no model."""
    set_tenant(state.organization_id)
    result = query_warehouse.invoke(
        {
            "relation": "goal_attainment",
            "filters": {"fiscal_period_id": state.fiscal_period_id},
            "order_by": "depth",
            "limit": 500,
        }
    )
    # Snowflake returns upper-cased identifiers; GoalNode ignores columns it
    # does not declare, so new mart columns will not break this node.
    goals = [
        GoalNode(**{key.lower(): value for key, value in row.items()})
        for row in result["rows"]
    ]
    return {"goals": goals}


def reconcile(state: RevOpsState) -> dict:
    """Check each parent against the sum of its SUM children.

    Deterministic on purpose: this is arithmetic, and a model that occasionally
    gets it wrong would undermine every report built on top of it.
    """
    by_parent: dict[str, list[GoalNode]] = {}
    for goal in state.goals:
        if goal.parent_goal_id:
            by_parent.setdefault(goal.parent_goal_id, []).append(goal)

    out: list[Reconciliation] = []
    for parent in state.goals:
        children = by_parent.get(parent.goal_id, [])
        if not children:
            continue
        summable = [c for c in children if c.relation_type == "SUM"]
        total = sum(c.target_value for c in summable) if summable else None
        variance = None if total is None else total - parent.target_value
        out.append(
            Reconciliation(
                parent_goal_id=parent.goal_id,
                fiscal_period_id=parent.fiscal_period_id,
                sum_of_children=total,
                parent_target=parent.target_value,
                variance=variance,
                is_balanced=(
                    variance is not None
                    and abs(variance) <= 0.005 * parent.target_value
                ),
                relation_types_present=sorted({c.relation_type for c in children}),
            )
        )
    return {"reconciliations": out}


def data_quality(state: RevOpsState) -> dict:
    """The veto. Findings built on stale or missing inputs publish as provisional."""
    unmeasurable = sorted(
        {g.metric_key for g in state.goals if g.source_status == "NO_SOURCE"}
    )
    total = len(state.goals) or 1
    gap_ratio = sum(1 for g in state.goals if g.source_status == "NO_SOURCE") / total

    if gap_ratio > 0.6:
        verdict, reason = "BLOCKED", (
            f"{gap_ratio:.0%} of goals have no measurable source. "
            "A report from this run would be mostly gaps."
        )
    elif unmeasurable:
        verdict, reason = "DEGRADED", (
            f"{len(unmeasurable)} metric(s) have no source: {', '.join(unmeasurable)}. "
            "Findings on those goals are provisional."
        )
    else:
        verdict, reason = "OK", "All goals in scope resolve to a computed source."

    return {
        "data_quality": DataQualityVerdict(
            is_fresh=True,
            max_staleness_hours=None,
            unmeasurable_metrics=unmeasurable,
            verdict=verdict,
            reason=reason,
        )
    }


# ---------------------------------------------------------------------- agents


def fan_out(state: RevOpsState) -> list[Send] | Literal["narrate"]:
    """Dispatch every department agent in parallel, unless data quality blocked."""
    if state.data_quality and state.data_quality.verdict == "BLOCKED":
        return "narrate"
    return [
        Send("department_agent", {"department": dept, "parent": state})
        for dept in DEPARTMENT_BRIEFS
    ]


def department_agent(payload: dict) -> dict:
    """One branch of the goal tree, explained by one agent."""
    department: Department = payload["department"]
    state: RevOpsState = payload["parent"]
    set_tenant(state.organization_id)

    agent = create_react_agent(
        _llm(),
        tools=[query_warehouse],
        prompt=f"{SHARED_RULES}\n\n{DEPARTMENT_BRIEFS[department]}",
        response_format=Finding,
    )
    result = agent.invoke(
        {
            "messages": [
                (
                    "user",
                    f"Tenant {state.organization_id}, period {state.fiscal_period_id}, "
                    f"as of {state.as_of_date}. Assess your branch against target and "
                    f"pace, and report the single most consequential finding.",
                )
            ]
        }
    )
    finding: Finding = result["structured_response"]
    finding.department = department
    finding.agent_name = f"{department.lower()}_agent"
    return {"findings": [finding]}


def narrate(state: RevOpsState) -> dict:
    """Assemble the exec summary. Reads findings only — never queries the warehouse."""
    dq = state.data_quality
    if dq and dq.verdict == "BLOCKED":
        return {
            "narrative": (
                f"Report withheld for {state.organization_id} / {state.fiscal_period_id}. "
                f"{dq.reason}"
            )
        }

    unbalanced = [r for r in state.reconciliations if not r.is_balanced]
    caveat = "" if not dq or dq.verdict == "OK" else f"\n\nProvisional: {dq.reason}"

    summary = _llm().invoke(
        [
            (
                "system",
                "You write the executive summary for a RevOps goal review. Lead with "
                "the outcome in one sentence, then the two or three things that need a "
                "decision. Write complete sentences for a reader who did not watch the "
                "run. Do not invent numbers — use only what is given.",
            ),
            (
                "user",
                f"Findings: {[f.model_dump() for f in state.findings]}\n"
                f"Unbalanced rollups: {[r.model_dump() for r in unbalanced]}\n"
                f"Data quality: {dq.model_dump() if dq else None}",
            ),
        ]
    )
    return {"narrative": summary.content + caveat}


def approval_gate(state: RevOpsState) -> dict:
    """Human-in-the-loop. Nothing writes back to the CRM without a decision."""
    pending = [f for f in state.findings if f.proposed_action]
    if not pending:
        return {"requires_approval": []}

    decision = interrupt(
        {
            "question": "Approve these proposed interventions?",
            "actions": [
                {"goal_id": f.goal_id, "action": f.proposed_action, "severity": f.severity}
                for f in pending
            ],
        }
    )
    approved = [f for f in pending if f.goal_id in set(decision.get("approved_goal_ids", []))]
    return {"requires_approval": approved}


def build_graph(checkpointer=None):
    graph = StateGraph(RevOpsState)

    graph.add_node("load_goals", load_goals)
    graph.add_node("reconcile", reconcile)
    graph.add_node("data_quality", data_quality)
    graph.add_node("department_agent", department_agent)
    graph.add_node("narrate", narrate)
    graph.add_node("approval_gate", approval_gate)

    graph.add_edge(START, "load_goals")
    graph.add_edge("load_goals", "reconcile")
    graph.add_edge("reconcile", "data_quality")
    graph.add_conditional_edges("data_quality", fan_out, ["department_agent", "narrate"])
    graph.add_edge("department_agent", "narrate")
    graph.add_edge("narrate", "approval_gate")
    graph.add_edge("approval_gate", END)

    return graph.compile(checkpointer=checkpointer or InMemorySaver())
