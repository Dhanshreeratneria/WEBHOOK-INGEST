# SOLUTION.md

## What was broken, and why

**1. Duplicate events / drifting call-counts.** `events.event_id` had a plain
index, not a unique one, and `Ingest()` deduplicated with a check-then-act:
`EventExists()` followed by a separate `InsertEvent()`. That pair is not
atomic. Two redeliveries of the same `event_id` arriving close together (the
provider explicitly does this) could both see "not present" before either had
written, both insert, and both call `IncrementAccountStats` — hence
duplicate rows and inflated counts. `internal/ingest/service_test.go`'s new
`TestConcurrentDuplicateDeliveriesAreIdempotent` fires the same delivery 20
times concurrently and asserts exactly one event and `call_count = 1`.

**2. Recordings never marked processed, nothing in the logs.**
`processRecording` ran in a detached goroutine but was passed `r.Context()`
— the *request's* context. `net/http` cancels that context as soon as the
handler returns, which is right around when the goroutine starts running, so
`MarkRecordingProcessed` almost always executed against an already-canceled
context and failed. On top of that, the error was thrown away
(`// TODO: handle`), so there was nothing to see in the logs either.
Two independent bugs, one symptom. Fixed by giving the background work its
own `context.Background()` with a bounded timeout, and by actually logging
the error.

**3. In-flight work disappearing on deploy.** `main.go`'s graceful shutdown
only called `srv.Shutdown()`, which waits for in-flight *HTTP requests* —
it has no idea the handler spawned a detached goroutine that's still
running. `Service` now tracks those goroutines with a `sync.WaitGroup` and
exposes `Shutdown(ctx)`, which `main.go` calls right after `srv.Shutdown()`.

**Bonus defect, not in the ops report but real:** `stats.Cache.Record` wrote
to the underlying map without taking `c.mu` at all (only `Get` locked it).
Under concurrent webhook traffic this is a genuine data race — lost updates
at best, a "concurrent map writes" panic at worst. `TestCacheRecordIsConcurrencySafe`
hits it with 200 concurrent writers; run with `go test -race` to see it flagged
directly.

## Deduplication strategy

I used a **Postgres unique constraint on `events.event_id`**, with
`INSERT ... ON CONFLICT (event_id) DO NOTHING RETURNING id` as the single
gate: if a row comes back, this delivery is new and everything downstream
(call upsert, stats increment, cache, recording processing) proceeds; if not,
it's a duplicate and `Ingest` returns early. That collapses the old
two-step check into one atomic statement, which is what actually closes the
race — the bug wasn't "no dedup check", it was "dedup check that isn't atomic
with the write".

**Why not Redis (`SETNX`)?** It's faster, but it isn't consistent with the
Postgres transaction. Redis could reasonably say "new event" and get evicted
or restart before the Postgres write completes, or say "seen" once and then
lose that key later, silently reopening the double-count window. Postgres is
already the durable source of truth for events and stats, so making it the
source of truth for dedup too avoids a second system that can disagree with
it. Redis is genuinely useful here as a *cache in front of* Postgres, not as
a *replacement* for it — see below.

**Why not an in-memory set / mutex in the Go process?** It doesn't survive a
restart and doesn't work once there's more than one instance of the service,
which is the direction any real deployment goes.

## At 10,000 webhooks/second

The unique-constraint approach stays correct at that volume but the
`SELECT`-shaped write on every request (even for the ~0% that are duplicates
in steady state) becomes wasted round-trips at that rate. I'd add:

- **A Redis pre-check as a fast path**, not a replacement: `SET event_id NX EX <ttl>`
  before hitting Postgres. A hit means "almost certainly a duplicate, skip
  the DB entirely"; a miss still goes through the Postgres `ON CONFLICT`
  insert as the real gate, so correctness never depends on Redis staying up.
  This turns the common case (first delivery) into one Redis round-trip
  instead of adding one, and turns hot-duplicate storms into an even cheaper
  Redis-only rejection.
- **Batching the recording-processing goroutines** through a bounded worker
  pool or a queue (the unbounded `go func()` per webhook is fine at low
  volume but is an unbounded-concurrency footgun at 10k/s).
- **Partitioning `account_stats` writes** or moving them off the synchronous
  request path (e.g. increment in Redis and flush to Postgres on an
  interval) if row-level lock contention on hot accounts becomes the
  bottleneck — that's a real trade-off against the durability guarantee I
  currently have, so I'd only make it if profiling actually showed
  contention there.

## What I'd do next if I had more time

- Add a metric/alert on the "duplicate delivery ignored" log line so a spike
  in redeliveries (which might indicate the provider is unhappy with our
  response times) is visible before ops has to file a report.
- Confirm the recording-processing timeout (currently a flat 30s) against
  real recording sizes rather than a guess.
