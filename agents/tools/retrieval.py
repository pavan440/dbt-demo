"""Dual-store retrieval for the RevOps knowledge base.

Two corpora with different governance needs, one router:

  Snowflake Cortex Search  small, hot, governed. Metric dictionary, benchmark
                           bands, goal-setting policy, comp rules. Inherits
                           Snowflake row-access policies, so tenant scoping is
                           enforced by the warehouse rather than by this code.

  S3 Vectors               large, cold, cheap. QBR decks, call transcripts,
                           campaign creative, prior-quarter board notes. Native
                           vector buckets driven through boto3, no cluster.

Merging is the part that goes wrong quietly. The two stores do not embed with
the same model and do not return comparable scores: S3 Vectors returns cosine
*distance* (lower is better) and Cortex Search returns its own relevance score
(higher is better). Min-max normalising those onto a shared scale invents a
comparison that does not exist, and the practical result is that the agent
silently prefers whichever store happens to produce larger numbers.

This module uses Reciprocal Rank Fusion instead, which combines *rankings*
rather than scores and needs no shared scale. Every hit carries the store it
came from and its within-store rank so a reader can see where an answer came
from.
"""

from __future__ import annotations

import json
import os
from typing import Any, Literal

import boto3
from langchain_core.tools import tool
from pydantic import BaseModel, Field

from agents.tools.snowflake import _TENANT, _connect

Store = Literal["cortex_search", "s3_vectors"]

EMBED_MODEL = os.environ.get("REVOPS_EMBED_MODEL", "amazon.titan-embed-text-v2:0")
EMBED_DIMENSION = int(os.environ.get("REVOPS_EMBED_DIMENSION", "1024"))
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
AWS_PROFILE = os.environ.get("AWS_PROFILE", "learning")

S3_VECTOR_BUCKET = os.environ.get("REVOPS_VECTOR_BUCKET", "wayplorer-revops-kb")
S3_VECTOR_INDEX = os.environ.get("REVOPS_VECTOR_INDEX", "revops-knowledge")
CORTEX_SERVICE = os.environ.get("REVOPS_CORTEX_SERVICE", "REVOPS_KB_SEARCH")

# RRF damping constant. 60 is the value from the original Cormack et al. paper
# and is the usual default; it stops a single store's top hit from dominating.
RRF_K = 60


def _session() -> boto3.session.Session:
    return boto3.session.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)


def embed(text: str) -> list[float]:
    """One embedding model for everything that lands in S3 Vectors.

    Cortex Search embeds internally with its own model, which is exactly why the
    two stores' scores must never be compared directly. See the module docstring.
    """
    client = _session().client("bedrock-runtime")
    response = client.invoke_model(
        modelId=EMBED_MODEL,
        body=json.dumps({"inputText": text, "dimensions": EMBED_DIMENSION}),
    )
    return json.loads(response["body"].read())["embedding"]


class Hit(BaseModel):
    doc_id: str
    text: str
    store: Store
    rank: int = Field(description="Rank within its own store, 1-based.")
    raw_score: float | None = Field(
        default=None,
        description="Store-native score. Not comparable across stores - present for audit only.",
    )
    fused_score: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


# --------------------------------------------------------------------- stores


def _query_s3_vectors(query: str, organization_id: str, top_k: int) -> list[Hit]:
    """Bulk corpus. Tenant scope is a server-side metadata filter, not a post-filter."""
    client = _session().client("s3vectors")
    try:
        response = client.query_vectors(
            vectorBucketName=S3_VECTOR_BUCKET,
            indexName=S3_VECTOR_INDEX,
            topK=top_k,
            queryVector={"float32": embed(query)},
            # Applied inside the service. A filter applied after retrieval would
            # let another tenant's document occupy a slot and silently truncate
            # this tenant's results even when nothing leaks.
            filter={"organization_id": organization_id},
            returnMetadata=True,
            returnDistance=True,
        )
    except client.exceptions.NotFoundException:
        # Index not provisioned yet. An empty bulk corpus is a normal state, not
        # an error - the governed store still answers.
        return []

    return [
        Hit(
            doc_id=v.get("key", ""),
            text=(v.get("metadata") or {}).get("text", ""),
            store="s3_vectors",
            rank=i + 1,
            raw_score=v.get("distance"),
            metadata=v.get("metadata") or {},
        )
        for i, v in enumerate(response.get("vectors", []))
    ]


def _query_cortex_search(query: str, organization_id: str, top_k: int) -> list[Hit]:
    """Governed corpus. Snowflake applies row-access policies on top of the filter."""
    payload = json.dumps(
        {
            "query": query,
            "columns": ["doc_id", "chunk_text", "doc_type", "industry"],
            "filter": {"@eq": {"organization_id": organization_id}},
            "limit": top_k,
        }
    )
    statement = (
        "select parse_json("
        "  snowflake.cortex.search_preview(%s, %s)"
        "):results as results"
    )
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(statement, [CORTEX_SERVICE, payload])
                row = cur.fetchone()
    except Exception:
        # Service not created yet, or Cortex not enabled on the account.
        return []

    if not row or not row[0]:
        return []

    results = json.loads(row[0]) if isinstance(row[0], str) else row[0]
    return [
        Hit(
            doc_id=r.get("doc_id", ""),
            text=r.get("chunk_text", ""),
            store="cortex_search",
            rank=i + 1,
            raw_score=r.get("score"),
            metadata={k: v for k, v in r.items() if k not in ("chunk_text",)},
        )
        for i, r in enumerate(results)
    ]


# ---------------------------------------------------------------------- fuse


def reciprocal_rank_fusion(rankings: list[list[Hit]], k: int = RRF_K) -> list[Hit]:
    """Combine rankings from scorers whose scores are not comparable.

    score(d) = sum over stores of 1 / (k + rank_of_d_in_that_store)

    A document found by both stores outranks one found by either alone, which is
    the behaviour you want from a governed store and a bulk store that overlap.
    """
    fused: dict[str, Hit] = {}
    for ranking in rankings:
        for hit in ranking:
            contribution = 1.0 / (k + hit.rank)
            existing = fused.get(hit.doc_id)
            if existing is None:
                hit.fused_score = contribution
                fused[hit.doc_id] = hit
            else:
                existing.fused_score = (existing.fused_score or 0.0) + contribution
                # Keep the governed store's copy of the text when both have it.
                if hit.store == "cortex_search":
                    existing.text = hit.text or existing.text
                    existing.metadata = {**existing.metadata, **hit.metadata}
    return sorted(fused.values(), key=lambda h: h.fused_score or 0.0, reverse=True)


# ---------------------------------------------------------------------- tool


class KnowledgeInput(BaseModel):
    query: str = Field(description="What you need to know, as a natural-language question.")
    top_k: int = Field(default=5, ge=1, le=20)


@tool("search_knowledge", args_schema=KnowledgeInput)
def search_knowledge(query: str, top_k: int = 5) -> dict[str, Any]:
    """Search the RevOps knowledge base for definitions, benchmarks and prior context.

    Use this for questions the warehouse cannot answer with a number: what a
    metric means, what a healthy benchmark looks like for this industry and
    segment, what was decided in a previous quarter, or what the playbook says
    to do about a given situation.

    Do not use it for figures - those come from query_warehouse. A benchmark
    retrieved here is context for interpreting a number, never a substitute for
    measuring one.
    """
    organization_id = _TENANT.get()
    per_store = max(top_k, 10)

    cortex = _query_cortex_search(query, organization_id, per_store)
    bulk = _query_s3_vectors(query, organization_id, per_store)
    merged = reciprocal_rank_fusion([cortex, bulk])[:top_k]

    return {
        "hits": [h.model_dump() for h in merged],
        "stores_queried": ["cortex_search", "s3_vectors"],
        "stores_answered": sorted({h.store for h in cortex + bulk}),
        "fusion": "reciprocal_rank_fusion",
        "organization_id": organization_id,
    }


# ------------------------------------------------------------------- ingest


def ensure_index() -> None:
    """Create the vector bucket and index if absent. Safe to call repeatedly."""
    client = _session().client("s3vectors")
    try:
        client.create_vector_bucket(vectorBucketName=S3_VECTOR_BUCKET)
    except client.exceptions.ConflictException:
        pass
    try:
        client.create_index(
            vectorBucketName=S3_VECTOR_BUCKET,
            indexName=S3_VECTOR_INDEX,
            dataType="float32",
            dimension=EMBED_DIMENSION,
            distanceMetric="cosine",
            # organization_id must stay filterable; text is retrieved, not filtered.
            metadataConfiguration={"nonFilterableMetadataKeys": ["text"]},
        )
    except client.exceptions.ConflictException:
        pass


def index_documents(documents: list[dict[str, Any]]) -> int:
    """Embed and store documents in S3 Vectors.

    Each document needs `doc_id`, `text` and `organization_id`. The tenant key is
    written as filterable metadata so retrieval can scope server-side; a document
    indexed without it is unreachable by any tenant-scoped query, which is the
    correct failure mode.
    """
    for doc in documents:
        missing = {"doc_id", "text", "organization_id"} - set(doc)
        if missing:
            raise ValueError(f"document missing required keys: {sorted(missing)}")

    client = _session().client("s3vectors")
    vectors = [
        {
            "key": doc["doc_id"],
            "data": {"float32": embed(doc["text"])},
            "metadata": {
                "organization_id": doc["organization_id"],
                "doc_type": doc.get("doc_type", "note"),
                "text": doc["text"],
                **{k: v for k, v in doc.items() if k not in ("doc_id", "text", "organization_id")},
            },
        }
        for doc in documents
    ]
    for start in range(0, len(vectors), 100):
        client.put_vectors(
            vectorBucketName=S3_VECTOR_BUCKET,
            indexName=S3_VECTOR_INDEX,
            vectors=vectors[start : start + 100],
        )
    return len(vectors)
