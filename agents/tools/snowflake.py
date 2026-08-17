"""Tenant-scoped Snowflake access for agents.

Security model
--------------
Agents never write SQL and never see a credential. They call a tool with
structured filters; this module builds the statement, binds the parameters, and
injects the ``organization_id`` predicate from the run context — not from
anything the model produced.

This is deliberate. A prompt instruction to "only query tenant X" is not an
access control: it is a suggestion the model can be talked out of. Cross-tenant
leakage in a RevOps report is the one failure that cannot be walked back, so the
tenant boundary is enforced here, in code, on every call.
"""

from __future__ import annotations

import hashlib
import os
from contextvars import ContextVar
from datetime import datetime
from typing import Any

import snowflake.connector
from langchain_core.tools import tool
from pydantic import BaseModel, Field

# Set by the graph runner before any agent node executes. Agents cannot read or
# write it, so there is no path by which a model can widen its own scope.
_TENANT: ContextVar[str] = ContextVar("organization_id")

# Only these relations are reachable. An agent asking for anything else gets a
# refusal from the tool, not a query against an unreviewed table.
ALLOWED_RELATIONS: dict[str, str] = {
    "goal_attainment": "MART_GOAL_ATTAINMENT",
    "goal_tree": "INT_GOAL_TREE",
    "metric_actual": "INT_METRIC_ACTUAL",
    "pipeline_health": "MART_TENANT_DAILY_PIPELINE_HEALTH",
    "module_snapshot": "MART_TENANT_MODULE_SNAPSHOT",
    "record_daily_activity": "INT_RECORD_DAILY_ACTIVITY",
    "fiscal_calendar": "DIM_FISCAL_CALENDAR",
}

# Columns an agent may filter on, per relation. Anything else is rejected —
# this is what stops a crafted filter name from becoming SQL injection.
ALLOWED_FILTER_COLUMNS: dict[str, set[str]] = {
    "goal_attainment": {
        "fiscal_period_id", "level", "metric_key", "owner_type", "owner_id",
        "pacing_status", "relation_type", "parent_goal_id", "goal_id", "source_status",
    },
    "goal_tree": {"fiscal_period_id", "level", "metric_key", "parent_goal_id", "goal_id", "rollup_status"},
    "metric_actual": {"fiscal_period_id", "metric_key", "owner_type", "owner_id"},
    "pipeline_health": {"kpi_date", "pipeline_health_status"},
    "module_snapshot": {"module_key"},
    "record_daily_activity": {"module_key", "record_date", "resolved_owner_id", "resolved_team_id"},
    "fiscal_calendar": {"fiscal_period_id", "calendar_date"},
}

MAX_ROWS = 500


def set_tenant(organization_id: str) -> None:
    """Bind the run to one tenant. Called by the runner, never by an agent."""
    _TENANT.set(organization_id)


def _connect() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "ANALYTICS"),
    )


class QueryResult(BaseModel):
    rows: list[dict[str, Any]]
    row_count: int
    truncated: bool
    query_hash: str
    relation: str
    retrieved_at: datetime


class QueryInput(BaseModel):
    relation: str = Field(description=f"One of: {', '.join(sorted(ALLOWED_RELATIONS))}")
    filters: dict[str, str | int | float] = Field(
        default_factory=dict,
        description="Equality filters, column -> value. Only allowlisted columns are accepted.",
    )
    columns: list[str] | None = Field(
        default=None, description="Columns to return. Omit for all columns."
    )
    order_by: str | None = None
    limit: int = Field(default=100, le=MAX_ROWS)


def _build_statement(payload: QueryInput, organization_id: str) -> tuple[str, list[Any]]:
    if payload.relation not in ALLOWED_RELATIONS:
        raise ValueError(
            f"Unknown relation {payload.relation!r}. Allowed: {sorted(ALLOWED_RELATIONS)}"
        )

    allowed_cols = ALLOWED_FILTER_COLUMNS[payload.relation]
    unknown = set(payload.filters) - allowed_cols
    if unknown:
        raise ValueError(
            f"Filters {sorted(unknown)} not permitted on {payload.relation}. "
            f"Allowed: {sorted(allowed_cols)}"
        )

    select_list = "*"
    if payload.columns:
        # Identifiers cannot be bound as parameters, so validate rather than interpolate blindly.
        for col in payload.columns:
            if not col.replace("_", "").isalnum():
                raise ValueError(f"Illegal column name {col!r}")
        select_list = ", ".join(payload.columns)

    relation = ALLOWED_RELATIONS[payload.relation]

    # The tenant predicate is first and is not derived from agent input.
    where_parts = ["organization_id = %s"]
    params: list[Any] = [organization_id]
    for col in sorted(payload.filters):
        where_parts.append(f"{col} = %s")
        params.append(payload.filters[col])

    statement = (
        f"select {select_list} from {relation} where " + " and ".join(where_parts)
    )
    if payload.order_by:
        if payload.order_by.replace("_", "").replace(" ", "").replace("desc", "").replace("asc", "").isalnum():
            statement += f" order by {payload.order_by}"
        else:
            raise ValueError(f"Illegal order_by {payload.order_by!r}")
    statement += f" limit {min(payload.limit, MAX_ROWS)}"

    return statement, params


@tool("query_warehouse", args_schema=QueryInput)
def query_warehouse(
    relation: str,
    filters: dict[str, str | int | float] | None = None,
    columns: list[str] | None = None,
    order_by: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """Query a governed RevOps relation for the current tenant.

    Call this whenever a claim depends on a number. Returns rows plus the
    lineage fields (query hash, row count, retrieval time) that every finding
    must carry as evidence.

    The tenant scope is applied automatically — do not pass an organization_id
    filter, and do not attempt to query another tenant's data.
    """
    organization_id = _TENANT.get()
    payload = QueryInput(
        relation=relation,
        filters=filters or {},
        columns=columns,
        order_by=order_by,
        limit=limit,
    )
    statement, params = _build_statement(payload, organization_id)

    with _connect() as conn:
        with conn.cursor(snowflake.connector.DictCursor) as cur:
            cur.execute(statement, params)
            rows = cur.fetchall()

    return QueryResult(
        rows=rows,
        row_count=len(rows),
        truncated=len(rows) >= min(payload.limit, MAX_ROWS),
        query_hash=hashlib.sha256(
            (statement + repr(params)).encode("utf-8")
        ).hexdigest()[:16],
        relation=payload.relation,
        retrieved_at=datetime.utcnow(),
    ).model_dump(mode="json")
