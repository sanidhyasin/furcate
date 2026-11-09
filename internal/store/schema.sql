-- Spans: one row each, append-only. Sorted by time, because dashboards scan time
-- ranges; single-trace lookups go through trace_index instead.
CREATE TABLE IF NOT EXISTS spans
(
    trace_id        String,
    span_id         String,
    parent_span_id  String,
    run_id          String,
    service         LowCardinality(String),
    kind            LowCardinality(String),
    name            String,
    model           LowCardinality(String),
    start_time      DateTime64(9, 'UTC') CODEC(Delta, ZSTD),
    end_time        DateTime64(9, 'UTC') CODEC(Delta, ZSTD),
    duration_ns     UInt64,
    tokens_in       UInt64,
    tokens_out      UInt64,
    cost_usd        Float64,
    status          LowCardinality(String),
    status_message  String,
    payloads        Map(LowCardinality(String), String),
    attributes      Map(LowCardinality(String), String) CODEC(ZSTD)
)
ENGINE = MergeTree
PARTITION BY toDate(start_time)
ORDER BY start_time;

-- trace_index: each trace's time range, so loading one trace reads one time slice
-- of spans instead of the whole table.
CREATE TABLE IF NOT EXISTS trace_index
(
    trace_id     String,
    first_start  SimpleAggregateFunction(min, DateTime64(9, 'UTC')),
    last_end     SimpleAggregateFunction(max, DateTime64(9, 'UTC'))
)
ENGINE = AggregatingMergeTree
ORDER BY trace_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS trace_index_mv TO trace_index AS
SELECT
    trace_id,
    min(start_time) AS first_start,
    max(greatest(start_time, end_time)) AS last_end
FROM spans
GROUP BY trace_id;

-- rollup_minute: per-minute stats by kind and model, so dashboards never scan spans.
CREATE TABLE IF NOT EXISTS rollup_minute
(
    minute      DateTime('UTC'),
    kind        LowCardinality(String),
    model       LowCardinality(String),
    spans       SimpleAggregateFunction(sum, UInt64),
    errors      SimpleAggregateFunction(sum, UInt64),
    latency     AggregateFunction(quantilesTDigest(0.5, 0.95, 0.99), UInt64),
    tokens_in   SimpleAggregateFunction(sum, UInt64),
    tokens_out  SimpleAggregateFunction(sum, UInt64),
    cost_usd    SimpleAggregateFunction(sum, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(minute)
ORDER BY (kind, model, minute);

CREATE MATERIALIZED VIEW IF NOT EXISTS rollup_minute_mv TO rollup_minute AS
SELECT
    toStartOfMinute(toDateTime(start_time, 'UTC')) AS minute,
    kind,
    model,
    count() AS spans,
    countIf(status = 'error') AS errors,
    quantilesTDigestState(0.5, 0.95, 0.99)(duration_ns) AS latency,
    sum(tokens_in) AS tokens_in,
    sum(tokens_out) AS tokens_out,
    sum(cost_usd) AS cost_usd
FROM spans
GROUP BY minute, kind, model;

-- payloads: prompts, completions and tool input/output, stored once by SHA-256.
-- Only `furcate trace -payloads` and replay read it; dashboards never do.
-- ReplacingMergeTree folds the rare duplicate insert (a retry, or a cache miss)
-- in the background.
CREATE TABLE IF NOT EXISTS payloads
(
    hash  String,
    size  UInt32,
    data  String CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree
ORDER BY hash;
