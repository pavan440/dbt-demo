# RevOps Goal-Management Agents

How the agent layer works, how to run it, and which parts are load-bearing.

Last updated: 2026-08-17

---

## What this is

A LangGraph application that reads the goal marts, explains variance per
department, and proposes interventions behind a human approval gate. It runs on
Claude via `langchain-anthropic`.

The important design property is that **most of the system is not agentic**.
Loading the goal tree, reconciling parents against children, and judging data
freshness are deterministic Python nodes. Agents are used only where the work is
genuinely interpretive. This is why LangGraph was chosen over CrewAI: CrewAI's
role/task/crew model pushes you toward making every step an agent, which is
slower, costlier, and non-reproducible for steps that should be pure functions.

```
load_goals ──▶ reconcile ──▶ data_quality ──┬─▶ [demand]      ─┐
   (code)       (code)         (code)       ├─▶ [pipeline]     │
                                            ├─▶ [forecast]     ├─▶ narrate ─▶ approval_gate
                                            ├─▶ [retention]    │   (agent)      (interrupt)
                                            └─▶ [efficiency]  ─┘
```

Bracketed nodes are agents. Everything else is code.

---

## Running it

```bash
python -m agents.run --tenant org_fintech_nova --period FY26-Q3
```

Required environment:

| Variable | Purpose |
|---|---|
| `ANTHROPIC_API_KEY` | Model access. Not currently set on this machine. |
| `SNOWFLAKE_ACCOUNT` / `USER` / `PASSWORD` / `ROLE` / `WAREHOUSE` / `DATABASE` | Warehouse access, held by the runner |
| `AWS_PROFILE` | Defaults to `learning`. Used for S3 Vectors and Bedrock embeddings. |
| `REVOPS_MODEL` | Optional. Defaults to `claude-opus-5`. |

The graph checkpoints, so a paused approval gate resumes later on the same
thread:

```bash
python -m agents.run --tenant org_fintech_nova --period FY26-Q3 \
  --thread <thread-id> --approve g_nova_q3_sales,g_nova_q3_sales_apac
```

> The default checkpointer is `InMemorySaver`, which means a resume only works
> within one process. Swap it for a durable saver before this runs on a
> schedule, or an interrupted approval is unrecoverable.

---

## Tenant isolation

Every table in this warehouse is keyed by `organization_id`. The isolation
boundary is enforced in `agents/tools/snowflake.py`, not in any prompt.

- The runner calls `set_tenant()` before the graph starts. It writes to a
  `ContextVar` that agents can neither read nor write.
- Agents never author SQL. They pass structured filters, and the tool builds the
  statement with `organization_id` bound from that context variable.
- Both the relation and every filter column are checked against an allowlist, so
  an unexpected filter name is a refusal rather than an injection point.
- S3 Vectors queries pass `filter={"organization_id": ...}` to the service, so
  scoping happens **inside** the store. A post-retrieval filter would let another
  tenant's document occupy a result slot and silently truncate this tenant's
  results even in the case where nothing actually leaks.

A prompt instruction to "only look at tenant X" is not an access control. It is
a suggestion the model can be talked out of, and cross-tenant leakage in a RevOps
report is the one failure that cannot be walked back.

---

## Prompt caching

Caching is a prefix match, so ordering is the whole design. A request renders
`tools`, then `system`, then `messages`.

The system prompt is split into two blocks:

| Block | Content | `cache_control` |
|---|---|---|
| 0 | `SHARED_RULES` — identical for all five department agents | `ephemeral` |
| 1 | The department brief — different per agent | none |

That makes the cached prefix `tool schemas + shared rules`, which is byte
identical across all five agents. Measured at roughly **715–954 tokens**, safely
above Claude Opus 5's 512-token minimum cacheable prefix. (That minimum is not
constant across models — it is 1024 on Opus 4.8 and 4096 on Opus 4.6, so a model
change can silently stop caching without any code change.)

**Where caching pays:** inside a single agent's ReAct loop. Every tool call
re-sends the whole prefix, so an agent that makes five warehouse queries would
otherwise pay for those tokens five times.

**Where it does not:** the first fan-out of a run. A cache entry only becomes
readable once the first response has begun streaming, so five agents dispatched
simultaneously all miss. That is expected behaviour, not a misconfiguration.

`cache_stats()` in `agents/graph.py` pulls the counters off a response. If
`cache_read` stays at zero across a multi-tool-call turn, something upstream of
the breakpoint is varying per request — a timestamp, a UUID, a reordered tool
list — and the prefix is being rebuilt every time.

---

## Knowledge base: two stores, one router

| | Snowflake Cortex Search | S3 Vectors |
|---|---|---|
| Corpus | Small, hot, governed | Large, cold, bulk |
| Content | Metric dictionary, benchmark bands, policy, comp rules | QBR decks, transcripts, campaign creative, board notes |
| Tenant scoping | Row-access policies, enforced by the warehouse | `filter` on `organization_id`, enforced by the service |
| Infrastructure | None — a Snowflake service | None — vector buckets via boto3 |

### The merge is the part that goes wrong quietly

The two stores do not embed with the same model and do not return comparable
scores. S3 Vectors returns cosine **distance** (lower is better); Cortex Search
returns its own relevance score (higher is better). Min-max normalising those
onto a shared scale invents a comparison that does not exist, and the practical
consequence is that the agent silently prefers whichever store produces larger
numbers.

`agents/tools/retrieval.py` uses **Reciprocal Rank Fusion** instead, which
combines rankings rather than scores and therefore needs no shared scale:

```
score(d) = Σ  1 / (k + rank of d within store i)
```

with `k = 60`. A document found by both stores outranks one found by either
alone, which is the behaviour you want when a governed store and a bulk store
overlap. Every hit carries its source store and within-store rank, so a reader
can see where an answer came from.

### Populating the bulk store

```python
from agents.tools.retrieval import ensure_index, index_documents

ensure_index()
index_documents([
    {"doc_id": "qbr-2026q2-nova", "organization_id": "org_fintech_nova",
     "doc_type": "qbr", "text": "..."},
])
```

`organization_id` is written as filterable metadata. A document indexed without
it is unreachable by any tenant-scoped query — which is the correct failure mode.

---

## Model selection

Every node currently runs `claude-opus-5`. If you later want to tune cost, the
natural split is by workload shape rather than by perceived importance:

| Node | Work | Candidate |
|---|---|---|
| Department agents | Query, read, extract, summarise one branch | A cheaper model — reading-heavy, little hard reasoning |
| Narrative agent | Synthesis for an executive reader | Opus 5 — this is the output people judge |
| Feasibility | Driver decomposition, judgment under uncertainty | Opus 5 |

Worth doing once real token usage is visible. Guessing at it now would be
premature, and changing model also invalidates the prompt cache.

---

## Status

Built and verified:

- Graph compiles; all six nodes wire up
- Typed state with an additive reducer, so parallel department agents do not
  clobber each other's findings
- Tenant-scoped warehouse tool with relation and filter-column allowlists
- Dual-store retrieval with RRF merge
- Cache block structure verified: identical across departments, above the minimum
- Runner with checkpoint resume and an approval gate

Not yet done:

- **The graph has never executed end to end** — there is no `ANTHROPIC_API_KEY`
  on this machine. Everything above is verified structurally, not behaviourally.
- Neither knowledge store is populated. `search_knowledge` returns empty and
  degrades quietly by design, so agents still function without it.
- `InMemorySaver` needs replacing with a durable checkpointer before scheduling.
- Agent findings are not yet persisted to the `agent_run` / `agent_finding`
  tables described in the blueprint; they currently live only in graph state.

---

## Related

- `models/marts/goal/mart_goal_attainment.sql` — target vs actual vs pace
- `models/marts/goal/mart_goal_feasibility.sql` — coverage-based feasibility
- `docs/wayplorer_snowflake_load_runbook.md` — how the warehouse gets loaded
