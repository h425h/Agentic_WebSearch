# Cerebro Search Engine

**Personal continuation of a CIS 5550 project at Penn (Spring 2026)**

A cloud-based search engine with a distributed web crawler, inverted index, PageRank ranking, and a planned agentic research layer that can synthesize multi-hop queries across the crawled corpus.

## Project Status

This is independent work building on a team project infrastructure. Current focus:

**Completed:**
- ✓ **Fix 1**: Collapsed HEAD+GET requests into single GET per URL (~2x network overhead reduction by eliminating duplicate DNS/TCP/TLS handshakes)
- ✓ **Fix 2**: Made crawler concurrency configurable via `--concurrency=N` job argument (enables partition tuning without recompilation)
- ✓ Both fixes validated on localhost with measured throughput improvements and partition counts

**In Progress:**
- Migrating to Azure for multi-worker cloud deployment ($100 student credit available)
- Scaling from ~100k to ~1M pages crawled (see `cerebro-crawl-scaleup-plan.md`)

**Planned:**
- **Fix 3**: Host-concentration diagnosis (requires analyzing frontier diversity in production runs)
- **Fix 4**: Worker scaling (add more workers once bottlenecks are clear)
- **Agentic search layer**: Multi-hop research queries with citation tracking (see `cerebro-agent-build-plan.md`)

**Infrastructure:**
- AWS Academy access expired; all validation so far is localhost-only (1 KVS worker, 1 Flame worker)
- Migration to Azure in progress for multi-worker testing

## Architecture

The system follows a distributed crawl-index-rank-serve pipeline:

```
Crawler (Flame job)  ──▶  pt-crawl   ──(Indexer)──▶  pt-index      ──┐
                                      ──(PageRank)─▶  pt-pageranks  ──┴─▶  Frontend
```

| KVS table      | Row key              | Columns                                  |
|----------------|----------------------|------------------------------------------|
| `pt-crawl`     | `Hasher.hash(url)`   | `url`, `page`, `responseCode`, `contentType`, `lang` |
| `pt-anchors`   | `Hasher.hash(url)`   | `text` (anchor text from incoming links) |
| `pt-index`     | word (lowercase)     | `acc` = url entries joined by SOH, each `url:tf:idf:pos1 pos2 ...` |
| `pt-pageranks` | `Hasher.hash(url)`   | `rank` (double, as string)               |

**KVS** (Key-Value Store): Distributed hash table with range partitioning, replication, and persistent storage.

**Flame**: MapReduce framework running on top of KVS, used for the crawler, indexer, and PageRank jobs.

**Frontend**: Web server that executes search queries by reading `pt-index` and `pt-pageranks`, computing TF-IDF scores, blending PageRank, and returning sorted results. Runs on the plain webserver (not Flame) for low-latency queries.

## Recent Improvements

### Fix 1: Single GET Request (2026-06-21)

**Problem**: Original implementation made two HTTP requests per URL (HEAD to check metadata, then GET to download). This roughly halved throughput due to duplicate DNS lookups, TCP handshakes, and TLS handshakes.

**Solution**: Collapsed into a single GET request with inline redirect handling. Headers (`Content-Type`, `Content-Length`, `Content-Language`) are available immediately after `getResponseCode()` — bail early before calling `getInputStream()` if content is non-HTML, oversized, or non-English.

**Impact**: ~2x network overhead reduction. Validated on localhost (621 pages, 2.1 pages/sec, 0 HEAD failures, stable connection count).

**Details**: `src/cis5550/crawler/Crawler.java:349-532`, documented in `CHANGES.md`

### Fix 2: Configurable Concurrency (2026-06-21)

**Problem**: The partitioner's concurrency multiplier was hardcoded at `4 * numWorkers`. Higher values create more parallel fetch tasks, but required recompilation to tune.

**Solution**: Added `--concurrency=N` job argument parsed in `Crawler.run()`:
```bash
java ... FlameSubmit localhost:9000 lib/crawler.jar cis5550.crawler.Crawler \
  http://example.com --concurrency=12 pt-blacklist
```

**Impact**: Enables partition tuning without rebuilding. Validated with partition counts (4 vs 12) matching expectations exactly.

**Details**: `src/cis5550/crawler/Crawler.java:123-151`, documented in `CHANGES.md`

## Source Layout

```
src/cis5550/
  webserver/   HTTP server (from HW1-3)      flame/       Flame coordinator + workers
  kvs/         KVS coordinator + workers      crawler/     Crawler.java (Flame job)
  generic/     Shared coordinator/worker base indexer/     Indexer.java (Flame job)
  tools/       HTTP, URLParser, Hasher, etc.  pagerank/    PageRank.java (Flame job)
  external/    PorterStemmer                  frontend/    Frontend.java + UI (HTML/CSS/JS)

config/   blacklist.txt, seed-urls.txt, stopwords.txt
static/   cerebro-mark.svg, cerebro-logo.png
deploy-ec2.sh   Cloud deployment script (AWS/Azure)
CHANGES.md      Detailed log of Fix 1 + Fix 2 validation results
cerebro-crawl-scaleup-plan.md    Plan to scale from ~100k to ~1M pages
cerebro-agent-build-plan.md      Plan for agentic research layer
```

## Building Locally

```bash
make            # Compiles all source to out/
```

Or manually:
```bash
javac -cp "lib/*" -d out src/cis5550/webserver/*.java src/cis5550/generic/*.java \
  src/cis5550/kvs/*.java src/cis5550/tools/*.java src/cis5550/external/*.java \
  src/cis5550/flame/*.java src/cis5550/crawler/Crawler.java

jar cf lib/crawler.jar -C out cis5550/crawler -C out cis5550/tools
```

No third-party dependencies. `lib/` contains prebuilt JARs for reference; `out/` shadows them on the classpath.

## Running Localhost Cluster

Start services in separate terminals:

```bash
# 1. KVS Coordinator
java -cp out cis5550.kvs.Coordinator 8000

# 2. Flame Coordinator
java -cp out cis5550.flame.Coordinator 9000

# 3. KVS Worker (storage in worker1/ directory)
java -cp out cis5550.kvs.Worker 8001 worker1 localhost:8000

# 4. Flame Worker
java -cp out cis5550.flame.Worker 9001 localhost:9000

# 5. Submit crawler
java -cp out:lib/* cis5550.flame.FlameSubmit localhost:9000 lib/crawler.jar \
  cis5550.crawler.Crawler http://example.com http://info.cern.ch --concurrency=12 pt-blacklist

# 6. Check progress (in separate terminal)
java -cp out cis5550.kvs.KVSClient localhost:8000
> count pt-crawl
> get pt-crawl <hash>
```

**Tuning knobs**:
- `--concurrency=N` in FlameSubmit command (default: 4)
- `export FLAME_CONCURRENCY=8` before starting Flame Worker (thread pool size, default: 4)

Stop all services: `pkill -f cis5550`

## Cloud Deployment (AWS/Azure)

`deploy-ec2.sh` supports multi-worker deployments on EC2 or Azure. Typical topology: 1 coordinator (runs KVS Coordinator :8000, Flame Coordinator :9000, Frontend :8080) + N workers (each runs KVS Worker :8001, Flame Worker :900x).

### Configuration

Create `.env` in repo root (git-ignored):
```bash
COORD=<coordinator public IP>
COORD_PRIVATE=<coordinator private IP>
WORKERS=<worker1 IP>,<worker2 IP>,<worker3 IP>,...
USER=ubuntu
SSH_KEY=/path/to/key.pem
NVME_MOUNT=/home/ubuntu/cerebro/kvsdata
```

### Commands

```bash
./deploy-ec2.sh deploy          # Fresh deployment: wipe data, deploy code, start services, submit crawler
./deploy-ec2.sh upgrade         # Re-upload code, restart services, KEEP KVS data
./deploy-ec2.sh run-indexer     # Submit Indexer job → builds pt-index
./deploy-ec2.sh run-pagerank    # Submit PageRank job → builds pt-pageranks
./deploy-ec2.sh run-frontend    # Start frontend on :8080
./deploy-ec2.sh stats           # Show per-worker + total crawl counts
./deploy-ec2.sh logs            # Tail coordinator logs
./deploy-ec2.sh stop            # Stop all services
```

Typical flow: `deploy` → wait for crawl → `run-indexer` → `run-pagerank` → `run-frontend` → visit `http://<COORD>/`

### Tuning for Larger Instances

Environment variables (defaults tuned for t3.medium with ~3.8 GB RAM):
```bash
FLAME_CONCURRENCY=4            # Threads per Flame worker (more concurrent KVS writes)
KVS_REPLICA_CONCURRENCY=1      # Replica-forwarder threads per KVS worker
WEBSERVER_NUM_WORKERS=200      # Frontend request-handler pool size

# Example: bump concurrency for c5.2xlarge (8 vCPU, 16 GB RAM)
FLAME_CONCURRENCY=12 ./deploy-ec2.sh upgrade
```

Also edit caps in source:
- `Indexer.java`: `MAX_PAGE_CHARS`, `MAX_WORDS_PER_PAGE` (bounds work per page)
- `PageRank.java`: `MAX_LINKS_PER_PAGE`, `MAX_ITERATIONS` (controls convergence)
- `deploy-ec2.sh`: JVM heap sizes (`-Xmx2g` for KVS, `-Xmx1g` for Flame)

## Next Steps

1. **Azure deployment**: Validate Fix 1 + Fix 2 improvements at scale (multi-worker cluster)
2. **Fix 3**: Diagnose host concentration in frontier with real crawl metrics
3. **Fix 4**: Add more workers if needed after measuring Fix 1-3 gains
4. **Agentic layer**: Build multi-hop research queries on top of the crawled corpus (see `cerebro-agent-build-plan.md`)

## References

- **Scale-up plan**: `cerebro-crawl-scaleup-plan.md` — detailed analysis of throughput bottlenecks and optimization roadmap
- **Change log**: `CHANGES.md` — validation results for Fix 1 (single GET) and Fix 2 (configurable concurrency)
- **Agentic search**: `cerebro-agent-build-plan.md` — design for multi-hop research layer with citation tracking

---

**Note**: This repository represents personal work building on a CIS 5550 team project. It has a fresh git history with no connection to the original course organization repo.
