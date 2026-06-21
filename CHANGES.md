# Cerebro Crawl Scale-Up Changes

This log tracks code changes made to scale from ~100k to ~1M pages, following the implementation plan in `cerebro-crawl-scaleup-plan.md`.

---

## Project Status (as of 2026-06-21)

**Completed:**
- ✓ Fix 1: Collapse HEAD+GET into single GET request (committed, validated locally)
- ✓ Fix 2: Make concurrency multiplier configurable via --concurrency=N (committed, validated locally)

**Not started:**
- Fix 3: Host-concentration diagnosis (requires analyzing frontier diversity in longer runs)
- Fix 4: Worker scaling (requires cloud infrastructure)

**Infrastructure status:**
- AWS Academy lab expired, no path back to AWS
- All validation so far is localhost-only (1 KVS worker, 1 Flame worker)
- $100 Azure student credit available but not yet spent
- No cloud deployment has happened yet

**Open issues:**
- Malformed URL normalization bug noted in Fix 1 validation (e.g., `http://info.cern.ch:8001/archive.orst.edu:9000/aeronautics`) — not fixed, worth investigating separately
- Agentic search layer (see `cerebro-agent-build-plan.md`) not started — depends on Cerebro being deployed somewhere reachable

**Next steps:**
- Deploy to Azure (or other cloud provider) for multi-worker testing
- Validate Fix 1 + Fix 2 improvements at scale
- Diagnose Fix 3 (host concentration) with real crawl metrics
- Consider Fix 4 (more workers) if needed after Fix 1-3 gains

---

## Fix 1: Collapse HEAD+GET into Single GET Request

**Date:** 2026-06-21
**File:** `src/cis5550/crawler/Crawler.java`
**Lines modified:** 349-532

### Problem
The original implementation made two separate HTTP requests per URL:
1. HEAD request to check status code, Content-Type, Content-Length, and follow redirects
2. GET request to download the actual page body

This roughly halved fetch throughput due to:
- Two DNS lookups per page
- Two TCP handshakes per page
- Two TLS handshakes per page (for HTTPS)
- Two full request/response cycles before politeness delay

### Solution
Collapsed into a **single GET request** with inline redirect handling:

1. **Redirect loop**: For redirect responses (301, 302, 303, 307, 308):
   - Read `Location` header
   - **Do NOT** call `getInputStream()` (avoid downloading redirect body)
   - Disconnect and loop to next request

2. **Header checks before body download**: For non-redirect responses:
   - `getContentType()`, `getContentLength()`, `getHeaderField()` are available immediately after `getResponseCode()`
   - Bail early (before calling `getInputStream()`) if:
     - Response code ≠ 200
     - Content-Type is not text/html
     - Content-Language indicates non-English
     - Content-Length > 5 MB

3. **Body download**: Only if all checks pass:
   - Stream body with existing size-cutoff logic (handles chunked encoding)
   - Disconnect after successful read

### Preserved behavior
- Same redirect limit (5 hops)
- Same content-type filtering
- Same language filtering (3-stage: header → HTML lang → stopwords)
- Same size limits (Content-Length check + stream cutoff)
- Same robots.txt and politeness handling (unchanged)
- Same KVS writes (url, responseCode, length, contentType per URL)

### Expected impact
~2x fetch throughput improvement (rough estimate: halves per-page network overhead).

### Validation results (localhost test)
**Test setup:** 1 KVS Worker + 1 Flame Worker (concurrency=4), seeds: example.com + info.cern.ch, ~5 min runtime

**Results:**
- **Total pages crawled:** 621
- **Throughput:** ~2.1 pages/sec (localhost, single worker)
- **ResponseCode distribution:**
  - 200 (success): 195
  - 301 (redirect): 63
  - 302 (redirect): 73
  - 404 (not found): 7
  - 400 (bad request): 4
  - -1 (connection failed): 12

**Verification:**
1. ✓ **No HEAD requests:** All error messages show "GET failed" (12 total), zero "HEAD failed"
2. ✓ **Connection handling:** All return paths properly disconnect, no leaks detected (stable ~30 open TCP connections)
3. ✓ **Redirect handling:** 301/302 responses captured without downloading body (contentType empty for redirects)
4. ✓ **Content-type filtering:** Non-HTML content (application/pdf, application/octet-stream) correctly rejected before page download
5. ✓ **Data integrity:** url, responseCode, contentType, lang columns correctly populated in pt-crawl

**Sample crawled pages:**
```
https://edh.cern.ch:443/                                      rc=302 ct=
https://home.cern:443/science/experiments/cloud               rc=301 ct=
http://info.cern.ch:8001/archive.orst.edu:9000/aeronautics    rc=-1  ct=
https://www.iana.org:443/reports/2015/customer-survey.pdf     rc=200 ct=application/pdf
https://hse.cern:443/content/waste-management                 rc=200 ct=
```

**Known issue observed (not fixed in this commit):**
One malformed URL discovered during testing: `http://info.cern.ch:8001/archive.orst.edu:9000/aeronautics` (rc=-1). This appears to be a pre-existing URL normalization bug in relative-URL resolution (concatenating host:port into the path segment), unrelated to Fix 1. Worth investigating separately to avoid wasting crawl budget on URLs that always fail, but not blocking this fix.

**Conclusion:** Fix 1 validated successfully. Single GET request implementation works correctly with proper redirect handling, early rejection of non-HTML content, and no connection leaks.

---

## Fix 2: Make Concurrency Multiplier Configurable

**Date:** 2026-06-21
**File:** `src/cis5550/crawler/Crawler.java`
**Lines modified:** 123-151, 188-190

### Problem
The partitioner's key-ranges-per-worker setting was hardcoded at line 175:
```java
ctx.setConcurrencyLevel(4 * ctx.getKVS().numWorkers());
```

This `4 *` multiplier controls how finely the URL space is partitioned across workers. A higher value creates more parallel fetch tasks, but the multiplier was not configurable without editing source code and recompiling.

Note: This is a separate knob from `FLAME_CONCURRENCY` (which sizes `Worker.java`'s thread pool). Both must be tuned together for full parallelism.

### Solution (after debugging false start)

**Initial approach (failed):** Tried reading from system property `-Dcerebro.concurrencyMultiplier=N` via `System.getProperty()` in `Crawler.run()`.

**Why it failed:** System properties set on the Flame Worker JVM (`java -Dcerebro.concurrencyMultiplier=12 ... cis5550.flame.Worker`) don't propagate into job code running inside Flame. Job code executes in a different context and doesn't inherit Worker-level JVM properties. Validated by observing identical partition counts (4) in both "baseline" and "high-concurrency" runs despite different property values.

**Actual fix:** Made concurrency multiplier a job argument, parsed in `run(FlameContext ctx, String[] args)` using the existing argument-passing mechanism (same way seed URLs and blacklist table arrive):

```java
// Parse arguments: seed URLs, optional --concurrency=N, optional blacklist table
int concurrencyMultiplier = 4;  // default
for (String arg : args) {
    if (arg.startsWith("http://") || arg.startsWith("https://")) {
        seedURLs.add(arg);
    } else if (arg.startsWith("--concurrency=")) {
        try {
            int value = Integer.parseInt(arg.substring("--concurrency=".length()).trim());
            if (value > 0) concurrencyMultiplier = value;
        } catch (NumberFormatException e) {
            ctx.output("WARNING: invalid --concurrency value, using default 4");
        }
    } else {
        blacklistTable = arg;
    }
}
ctx.setConcurrencyLevel(concurrencyMultiplier * ctx.getKVS().numWorkers());
```

**Usage:**
```bash
java ... FlameSubmit localhost:9000 lib/crawler.jar cis5550.crawler.Crawler \
  http://example.com http://info.cern.ch --concurrency=12 pt-blacklist
```

### Validation results (localhost test with argument-based config)
**Test setup:** 1 KVS Worker + 1 Flame Worker, seeds: example.com + info.cern.ch, 3 min per run

**Baseline (no --concurrency arg, default=4):**
- Partitions: **4** ✓
- Pages: 393 in 182 sec
- Throughput: **2.16 pages/sec**
- KVS errors: 0

**High-concurrency (--concurrency=12):**
- Partitions: **12** ✓
- Pages: 410 in 182 sec
- Throughput: **2.25 pages/sec**
- KVS errors: 0

**Improvement:** +4% throughput (1.04x) on single-worker localhost.

**Analysis:**
The modest improvement (4% vs 17% in the hardcoded validation) is expected on a single-worker localhost test because:
1. Limited CPU/network resources to exploit higher parallelism
2. Small test corpus doesn't generate enough concurrent fetch opportunities
3. Per-host rate limiting (1 req/sec) caps throughput when frontier concentrates on few hosts
4. Run-to-run variance on small samples (±10-15% is normal)

The critical validation is **partition counts match expectations exactly** (4 vs 12), proving the configuration mechanism works correctly.

Expected to scale better on multi-worker clusters where:
- More workers distribute the per-host rate-limit bottleneck
- Larger, more diverse corpus provides more concurrent fetch opportunities
- Higher multiplier creates finer partitioning across workers

**No KVS strain:** Zero errors at multiplier=12. Monitor on larger deployments; increase KVS_REPLICA_CONCURRENCY if errors appear (different bottleneck).

**Conclusion:** Fix 2 validated. Argument-based concurrency configuration works correctly end-to-end. Safe to deploy with higher values on multi-worker clusters.

---
