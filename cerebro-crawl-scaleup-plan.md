# Cerebro Crawl Scale-Up Plan

Target: get from ~100k pages to ~1M pages, on a limited AWS Academy credit budget, before
starting the agentic layer. This is a prerequisite phase — "Phase 0" relative to the agent
build plan.

This document is grounded in your actual code (`Crawler.java`, `flame/Worker.java`,
`deploy-ec2.sh`) — every claim below points at a specific line of behavior, not a guess.

---

## 1. Diagnosis: why ~100k, not ~1M

Five things are compounding. None require a rewrite — all are tuning or small, targeted
code changes. Ordered by expected impact:

### (a) HEAD + GET = two full round-trips per page
`Crawler.java`'s flatMap lambda does a `HEAD` request first (to check status/content-type/
redirects), and only if that passes, a separate `GET` for the body. Every single crawled
URL pays for two DNS lookups, two TCP handshakes, two TLS handshakes (for https), and two
full request/response cycles — before any politeness delay is even applied. This roughly
halves your real fetch throughput compared to a single-request design.

**Why it was probably written this way:** to avoid downloading large/non-HTML bodies you'll
just discard, and to follow redirects before committing to a GET. Reasonable instinct, but
costly at the scale you need.

### (b) Per-host rate limiting is 1 request/second, hard-capped
`delayMs = 1000` is the default (overridden upward by `Crawl-delay` from robots.txt, capped
at 30s). Combined with `MAX_PAGES_PER_HOST = 300`, if your seed set or link graph
concentrates traffic on a relatively small number of hosts (very common — Wikipedia,
major news sites, etc. dominate most general crawls), you are throughput-capped at
**~1 page/sec per host**, no matter how many threads or workers you add. More parallelism
only helps if it's spread across *more distinct hosts*, not more requests to the same ones.

### (c) `FLAME_CONCURRENCY` defaults to 4
`ctx.setConcurrencyLevel(4 * ctx.getKVS().numWorkers())` in `Crawler.java` sets key-ranges-
per-worker to `4 * numWorkers`. But `deploy-ec2.sh` reads `FLAME_CONCURRENCY` (default `4`)
into `-Dflame.worker.concurrency`, and `Worker.java`'s actual thread pool (`NUM_THREADS`,
default 32) is the thing that determines how many of those partitions can run truly
concurrently within a worker process. If you deployed without explicitly setting
`FLAME_CONCURRENCY` higher, you were leaving most of that 32-thread pool idle — the
partitioning (how the URL space is sliced) was coarser than the thread capacity.
`.env.example` only shows `FLAME_CONCURRENCY=64` as a *commented-out* suggestion.

### (d) Worker count
The example config (`.env.example`) lists 4 worker IPs. The project handout's own math
("five workers can crawl more than a million pages in about ten hours" at 5 pages/sec/
worker) assumes both more workers and higher per-worker fetch parallelism than a
4-worker / concurrency-4 setup delivers.

### (e) Frontier overhead at scale (secondary)
Each iteration does `count()` before, `distinct()`, `filter()` with a per-unique-URL
`existsRow` check, possibly `sample()`, then `count()` after. This is per-iteration
overhead, not per-page — it matters at very large frontier sizes (your cap is 50,000) but
is a smaller lever than (a)–(d). Don't spend early effort here.

**Bottom line:** (a) and (c) are the two highest-leverage fixes, and both are achievable
without spending more AWS credit. (d) costs money. (b) is partly a politeness requirement
you shouldn't break, but there's real headroom in *how* you use the time you're already
budgeting per host.

---

## 2. Fix plan, in priority order

### Fix 1 — Collapse HEAD+GET into one request (biggest lever, ~halves fetch cost)

Replace the HEAD-then-GET sequence with a single `GET`, reading response code,
`Content-Type`, and `Content-Length` from the same connection you read the body from.
Handle redirects by following `Location` headers manually in a loop (same redirect-cap
logic you already have, just against GET responses instead of HEAD responses) — abort
the body read early (don't call `getInputStream()`) on a redirect response code, so you
don't download bodies you're about to discard anyway.

**Why this is safe to change:** it doesn't touch politeness, robots.txt handling, language
filtering, or any of the dedup/content-hash logic — it only changes how many requests it
takes to get the same information you already extract today (status, headers, body).

**One subtlety to preserve:** your current code rejects non-HTML and oversized content
*before* downloading the body (via HEAD's `Content-Length`/`Content-Type` headers). With
GET-only, you still get headers before the body streams in (`getContentType()` and
`getContentLength()` are available right after `getResponseCode()`, before you touch
`getInputStream()`), so you can still bail out early on bad content-type without paying
for the download. Content-Length absence (chunked encoding) means you still rely on the
existing `MAX_CONTENT_SIZE` stream-read cutoff — that logic doesn't change.

**Expected impact:** roughly 2x fetch throughput, for free.

### Fix 2 — Raise `FLAME_CONCURRENCY` explicitly, and re-derive `NUM_THREADS` headroom

Set `FLAME_CONCURRENCY` explicitly in `.env` (start at 16–24, not the commented-out 64 —
see "how to tune without guessing" below) rather than relying on the default of 4. This
is a deploy-time config change, zero code risk. Confirm `flame.worker.concurrency` actually
shows up in worker startup logs (`deploy-ec2.sh` prints it: `"flame.worker.concurrency=" +
NUM_THREADS` is logged from `Worker.java` main — wait, that line logs `NUM_THREADS` itself,
not the concurrency-level partitioning; check both: the worker's own thread pool size
(`flame.worker.concurrency` system property → `NUM_THREADS`) AND the partitioner's
key-ranges-per-worker (`ctx.setConcurrencyLevel`, hardcoded to `4 * numWorkers` in
`Crawler.java`, not currently read from an env var at all).

**Action needed beyond just bumping the .env var:** `Crawler.java` line ~175 hardcodes
`4 * ctx.getKVS().numWorkers()` — this is a separate knob from `FLAME_CONCURRENCY` and is
NOT currently configurable via environment variable. To actually raise key-ranges-per-worker
you need to either change that multiplier in code (e.g. `8 *` or `12 *`) or make it
read from a system property the same way `deploy-ec2.sh` already passes others through.
This is a one-line code change plus a redeploy.

### Fix 3 — Diversify the frontier so per-host rate limiting doesn't dominate

The 1 req/sec/host ceiling is fine *if* the frontier is spread across many hosts at once.
Check (don't guess) whether your current crawl is host-concentrated: run with
`CEREBRO_DIAG=true` for a 10-15 minute window and look at `HOST_PAGE_COUNT` growth pattern
in the diagnostic logs, or just inspect `pt-hosts` row count vs `pt-crawl` row count after
a short test crawl — a low ratio (few hosts, many pages) confirms host concentration is
your bottleneck, not just raw thread count.

If concentrated: consider lowering `MAX_PAGES_PER_HOST` (forces breadth sooner, pushes the
frontier toward new hosts faster) or seeding from a more diverse set of starting domains so
the early frontier naturally spreads out before host caps kick in.

### Fix 4 — Worker count and instance type (the one that costs money — use last, use carefully)

Only reach for this after Fixes 1–3, since it's the expensive lever and you said credit is
limited. First, **check your actual `.env`** (not in the zip — git-ignored) to confirm what
you're currently running: instance type, worker count. The deploy script supports both
`t3.medium` (commented-out EBS fallback path) and `i4i.large` (NVMe path) — confirm which
you're actually on before deciding whether to upsize.

If you do add capacity: more **workers** (parallel fetch capacity, spreads host-load
naturally) generally beats bigger **instances** (more threads per worker, but you're
probably not CPU-bound — you're I/O-wait-bound on network requests) for a crawler
specifically. Going from 4 to 6-8 t3.medium-class workers is usually cheaper than 4
bigger instances and better matches what's actually the bottleneck (concurrent open
connections, not compute).

---

## 3. How to tune without guessing — a short test-crawl loop

Don't jump straight to a 1M-page overnight run with new settings untested — this repeats
the exact mistake your own report flags in §4 ("we tried to run on the full crawl too
early... most of our hardest bugs were found on a small corpus").

1. Apply Fix 1 (HEAD+GET → GET-only) alone. Run a short crawl (15-20 min, small seed set)
   with `CEREBRO_DIAG=true`. Compare pages/sec against a baseline run with the old code on
   the same seeds. Confirm no regressions in content-type filtering or redirect handling
   (spot-check a handful of crawled rows for correct `responseCode`/`contentType`).
2. Add Fix 2 (concurrency bump) on top. Re-run the same short test. Watch for KVS worker
   strain (errors in `kvs-w*.log`) — if raising concurrency causes write failures or
   timeouts, you've found the next bottleneck (KVS write throughput), not crawler fetch
   throughput, and the fix is different (e.g. `KVS_REPLICA_CONCURRENCY`).
3. Only after 1+2 are validated on a short run, commit to a longer run (a few hours) and
   check the pages/sec trend holds, not just the first few minutes.
4. Only then decide whether Fix 4 (more/bigger machines) is actually still needed to hit
   ~1M in your remaining time, or whether 1+2+3 already gets you there.

This loop costs you an afternoon, not a day, and avoids burning AWS credit on a long run
configured wrong.

---

## 4. Rough math — is 1M actually reachable on your timeline?

Using the handout's own estimate (5 pages/sec/worker → 1M pages in ~10 hours with 5
workers) as a sanity baseline, and assuming Fix 1 alone roughly doubles realistic per-worker
throughput from whatever your current ~100k-page run achieved:

- If your last run got ~100k pages in some known wall-clock time, you have a real baseline
  pages/sec number — use it. (Check `pt-crawl` row count + your crawl's actual run
  duration from logs; don't estimate this, look it up.)
- Doubling that via Fix 1, then another meaningful jump via Fix 2 (concurrency was likely
  underutilized at the default), a 5-10x combined improvement from code/config fixes alone
  is a reasonable expectation before touching worker count at all.
- If your current baseline is, say, ~5k pages/hour, a 5-10x improvement puts you at
  25k-50k pages/hour — i.e. 1M pages in roughly 20-40 hours of crawl wall-clock time, which
  is very achievable left running over 1-2 days without needing more machines.

This is intentionally a rough estimate, not a promise — the point is that **code fixes
likely get you most of the way to 1M before you need to spend more credit on workers**,
which matters given your stated budget constraint.

---

## 5. What NOT to touch

- **Don't relax robots.txt or politeness handling.** Per the assignment handout: "Any
  complaints we receive will result in major deductions." The 1 req/sec/host floor is a
  feature, not a bug — work around it via frontier diversity (Fix 3), not by ignoring it.
- **Don't raise `MAX_CONTENT_SIZE` or remove the language/blacklist filters** to inflate
  page count with junk — grading explicitly weighs "quality of search results," and a
  bigger but lower-quality corpus likely scores worse, not better.
- **Don't skip the small-scale validation loop in §3** even though it feels slow when the
  deadline is close — your own report's #1 lesson (§4) is that skipping this is what
  caused the worst, most time-consuming bugs last time.

---

## 6. Checklist

- [ ] Confirm actual current `.env` (instance type, worker count) — not visible in the zip
- [ ] Look up actual baseline: pages crawled ÷ actual wall-clock crawl duration from logs
- [ ] Implement Fix 1 (GET-only, redirects handled inline) — validate on short test crawl
- [ ] Implement Fix 2 (raise key-ranges-per-worker — code change, not just .env) — validate
- [ ] Check host-concentration (Fix 3 diagnosis) — adjust `MAX_PAGES_PER_HOST` or seeds if needed
- [ ] Run a multi-hour test crawl, confirm pages/sec trend holds over time, not just at start
- [ ] Decide, with real numbers in hand, whether Fix 4 (more/bigger workers) is still needed
- [ ] Only once corpus is at target size: re-run indexer + PageRank, sanity-check ranking
      quality hasn't degraded (your existing ~30 sample queries + `/debug` endpoint)
- [ ] Then move to the agentic layer build plan (separate document)
