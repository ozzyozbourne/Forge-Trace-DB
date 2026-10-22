# Forge HTTP API

All endpoints are on port 8080 unless noted. With auth enabled, send `x-api-key: <key>` or
`Authorization: Bearer <key>`. Endpoints that take `project` default to the key's project (or,
for admin / auth disabled, `x-forge-project` or `ingest.default_project`). Errors are
`{"error": "..."}` with 400 (bad input), 401, 404, 429 (`Retry-After: 1`), 504 (query timeout)
or 500.

Times are accepted as RFC 3339 or as epoch integers (ns, ms or s by magnitude) and returned as
RFC 3339 UTC with nanoseconds. Ids are lowercase hex (trace ids also accept UUID form).

## Ingest

| Method | Path | Body |
| --- | --- | --- |
| gRPC :4317 | `opentelemetry.proto.collector.trace.v1.TraceService/Export` | OTLP |
| POST :4318 | `/v1/traces` | OTLP protobuf (`application/x-protobuf`) or JSON (`application/json`), optionally gzip |
| POST | `/v1/runs` | one run (below) |
| PATCH | `/v1/runs/{run_id}` | fields to update; include `trace_id` unless the run is still unflushed |
| POST | `/v1/runs/batch` | `{"post": [run...], "patch": [run...]}` |

A run: `id`, `trace_id` (omit for a root run: its trace id is its own id), `parent_run_id`,
`name`, `run_type`, `start_time`, `end_time`, `inputs`, `outputs`, `error`, `status`, `tags`,
`extra.metadata` (or `metadata`), `session_id`/`thread_id`, `model`, `provider`,
`prompt_tokens`, `completion_tokens`, `total_tokens`, `dotted_order`, `project` (or
`session_name`), `deleted`. Responses are `202 {"accepted": n}`.

## Traces

`GET /v1/traces/{trace_id}?project=&payloads=true`

```json
{
  "project_id": "support",
  "trace_id": "5b8efff798038103d269b633813fc60c",
  "span_count": 4,
  "spans": [
    {
      "span_id": "eee19b7ec3c1b174", "parent_span_id": null, "name": "LangGraph.workflow",
      "run_type": "chain", "start_time": "2025-10-22T16:00:00.000000000Z", "end_time": "...",
      "latency_ms": 4200.0, "status": "ok", "error": null, "model": null, "provider": null,
      "prompt_tokens": null, "completion_tokens": null, "total_tokens": null, "cost_usd": null,
      "session_id": "thread-42", "tags": [], "metadata": {"user_id": "u-7"},
      "inputs": {...}, "outputs": {...}, "events": null, "attributes": {...},
      "children": [ ... ]
    }
  ]
}
```

`payloads=false` omits `inputs`, `outputs`, `events` and `attributes` (and never reads them).

`GET /v1/traces/{trace_id}/spans/{span_id}?project=` returns one span with payloads.

## Runs

`GET /v1/runs?project=&start=&end=&run_type=&status=&name=&session_id=&model=&tag=&min_latency_ms=&max_latency_ms=&min_tokens=&max_tokens=&min_cost=&max_cost=&metadata.<key>=<value>&trace_id=&all_spans=false&q=&limit=50&cursor=`

Root spans (all spans with `all_spans=true`), newest first, without payloads:
`{"runs": [span...], "next_cursor": "..."}`. Pass `next_cursor` back as `cursor` for the next
page. `q` switches to full-text search with the same filters.

## Search

`POST /v1/search`

```json
{
  "query": "refund policy",          // words are ANDed; "a OR b"; "error:timeout" picks a column
  "project": "support",
  "column": "inputs",                // optional: name, error, inputs, outputs, metadata
  "path": "$.messages[*].content",   // optional: only text under this JSON path of `column`
  "roots_only": false,
  "filters": { "status": "error", "start": "2025-10-22T00:00:00Z" },   // RunFilter fields
  "limit": 50,
  "cursor": null
}
```

Returns `{"runs": [...], "next_cursor": ..., "elapsed_ms": ...}`, newest first.

## SQL

`POST /v1/sql {"query": "...", "project": null, "format": "json" | "table"}`

Read-only: DDL, DML and `SET` are rejected. `EXPLAIN` works. The table is `spans`
(see `docs/architecture.md` for columns). Functions:

| Function | Meaning |
| --- | --- |
| `json_key(col, '$.a.b[0]')` | value at a JSON path, as text (arrays/objects as JSON) |
| `json_key_search(col, '$.messages[*].content', 'text')` | tokens of `text` all appear under that path |
| `search(col, 'query')` | full-text match on one column (same syntax as `/v1/search`) |
| `trace_tree('<trace id>' [, '<project>'])` | table: `depth` + all span columns, in tree order |

Binary ids are returned as hex. Example:

```sql
SELECT model, count(*) AS calls, sum(cost_usd) AS usd, approx_percentile_cont(
         arrow_cast(end_time, 'Int64') - arrow_cast(start_time, 'Int64'), 0.95) / 1e6 AS p95_ms
FROM spans
WHERE run_type IN ('llm', 'chat_model') AND start_time > now() - INTERVAL '1 day'
GROUP BY model ORDER BY usd DESC;
```

JSON response: `{"columns": [{"name", "type"}], "rows": [{...}], "row_count", "elapsed_ms"}`.

## Projects (admin)

| Method | Path | |
| --- | --- | --- |
| POST | `/v1/projects` | `{"id", "name"?, "retention_days"?}` → `201 {"id", "api_key"}` |
| GET | `/v1/projects` | projects visible to the caller |
| POST | `/v1/projects/{id}/rotate-key` | → `{"id", "api_key"}` |

## Ops (port 9090)

`GET /metrics`, `GET /healthz`, `GET /readyz`.
