"""CLI entrypoint for a RevOps goal review.

    python -m agents.run --tenant org_fintech_nova --period FY26-Q3

Environment required:
    ANTHROPIC_API_KEY                model access
    SNOWFLAKE_ACCOUNT/USER/PASSWORD/ROLE/WAREHOUSE/DATABASE/SCHEMA
    AWS_PROFILE                      defaults to "learning", for S3 Vectors and Bedrock

The graph resumes from a checkpoint, so an interrupted approval gate can be
answered in a later invocation with the same --thread.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import date

from langgraph.types import Command

from agents.graph import build_graph
from agents.state import RevOpsState
from agents.tools.snowflake import set_tenant


def _require_env() -> list[str]:
    needed = [
        "ANTHROPIC_API_KEY",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
    ]
    return [name for name in needed if not os.environ.get(name)]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a RevOps goal review for one tenant.")
    parser.add_argument("--tenant", required=True, help="organization_id to scope the run to")
    parser.add_argument("--period", required=True, help="fiscal_period_id, e.g. FY26-Q3")
    parser.add_argument("--as-of", default=None, help="YYYY-MM-DD, defaults to today")
    parser.add_argument("--thread", default=None, help="Checkpoint thread id, for resuming")
    parser.add_argument(
        "--approve",
        default=None,
        help="Comma-separated goal_ids to approve, resuming a paused approval gate",
    )
    parser.add_argument("--json", action="store_true", help="Emit the full state as JSON")
    args = parser.parse_args(argv)

    missing = _require_env()
    if missing:
        print(f"Missing environment: {', '.join(missing)}", file=sys.stderr)
        return 2

    # Bind the tenant before the graph runs. Agents cannot reach this context
    # variable, so no prompt can widen the scope of a single query.
    set_tenant(args.tenant)

    graph = build_graph()
    thread_id = args.thread or str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}}

    if args.approve is not None:
        approved = [g.strip() for g in args.approve.split(",") if g.strip()]
        result = graph.invoke(Command(resume={"approved_goal_ids": approved}), config)
    else:
        state = RevOpsState(
            organization_id=args.tenant,
            fiscal_period_id=args.period,
            as_of_date=date.fromisoformat(args.as_of) if args.as_of else date.today(),
            run_id=thread_id,
        )
        result = graph.invoke(state, config)

    if args.json:
        print(json.dumps(result, indent=2, default=str))
        return 0

    print(f"\nthread: {thread_id}")
    dq = result.get("data_quality")
    if dq:
        print(f"data quality: {dq.verdict} - {dq.reason}")

    unbalanced = [r for r in result.get("reconciliations", []) if not r.is_balanced]
    print(f"goals: {len(result.get('goals', []))}   unbalanced rollups: {len(unbalanced)}")

    print("\nfindings")
    for finding in result.get("findings", []):
        print(f"  [{finding.severity:<8}] {finding.department or '-':<10} {finding.claim}")

    if result.get("narrative"):
        print(f"\n{result['narrative']}")

    pending = result.get("requires_approval") or []
    if pending:
        print(f"\nawaiting approval ({len(pending)}). Resume with:")
        ids = ",".join(f.goal_id or "" for f in pending)
        print(f"  python -m agents.run --tenant {args.tenant} --period {args.period} "
              f"--thread {thread_id} --approve {ids}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
