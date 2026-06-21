# Cerebro Search Engine

CIS 5550 Final Project — Spring 2026

A cloud-based search engine: a web crawler builds a corpus in the KVS, Flame jobs turn that
corpus into an inverted index and PageRank scores, and a web frontend serves ranked queries.

## Team
- Hashem Awad — search UI (HTML/CSS/JS)
- Harsh Mishra — KVS / Flame, Frontend server
- Marcos de la Mo — crawler, EC2 deployment
- Tony Sui — Indexer, PageRank

## Pipeline

```
crawler  ──▶  pt-crawl   ──(Flame Indexer)──▶  pt-index      ──┐
                          ──(Flame PageRank)─▶  pt-pageranks  ──┴─▶  Frontend (HW1/2/3 webserver, no Flame)
```

| KVS table      | row key              | columns                                  |
|----------------|----------------------|------------------------------------------|
| `pt-crawl`     | `Hasher.hash(url)`   | `url`, `page`, `responseCode`, `contentType` |
| `pt-anchors`   | `Hasher.hash(url)`   | `text` (anchor text of links pointing to this url) |
| `pt-index`     | the word (lowercase) | `acc` = url entries joined by SOH (char 1), each `url:tf:idf:pos1 pos2 ...` |
| `pt-pageranks` | `Hasher.hash(url)`   | `rank` (double, as string)               |

The frontend reads `pt-index` and `pt-pageranks`, computes TF·IDF, blends in PageRank, and
returns sorted results. It runs on the plain webserver — **never** Flame (too slow for queries).

## Source layout

```
src/cis5550/
  webserver/   HW1-3 web server          flame/       Flame coordinator + workers
  kvs/         KVS coordinator + workers crawler/     Crawler.java
  generic/     shared framework iface     indexer/     Indexer.java   (package cis5550.jobs)
  tools/       HTTP, URLParser, Hasher…   pagerank/    PageRank.java  (package cis5550.jobs)
  external/    PorterStemmer              frontend/    Frontend.java + home.html / styles.css / app.js
config/   blacklist.txt, seed-urls.txt, stopwords.txt
static/   cerebro-mark.svg etc.
deploy-ec2.sh   AWS EC2 deploy / run script
```

## Building locally

```
make            # or: javac -cp "lib/*" --source-path src -d out <files>
```
No third-party libraries are used; `out/` shadows the (older) prebuilt `lib/*.jar` on the
classpath, so the build is effectively from source. `lib/` and `out/` are git-ignored.

## Deploying on EC2

The whole pipeline runs on EC2 via `deploy-ec2.sh`. Topology: **1 coordinator** (runs the KVS
Coordinator :8000, the Flame Coordinator :9000, the Frontend :8080, and an iptables 80→8080
redirect) **+ 4 workers** (each runs a KVS Worker :8001 and a Flame Worker :900x). The script
compiles everything *from source on each host* (the prebuilt jars are a newer Java version than
the AMI's JDK), so all you need on the instances is JDK 21 (the script installs it if missing)
and SSH.

### 1. Configure `.env`

`deploy-ec2.sh` reads connection details from a git-ignored `.env` in the repo root:

```
COORD=<coordinator public IP>
COORD_PRIVATE=<coordinator private IP, e.g. 172.31.x.x>
WORKERS=<worker1 public IP>,<worker2>,<worker3>,<worker4>   # at least 4
USER=ubuntu
SSH_KEY=/absolute/path/to/labsuser.pem
NVME_MOUNT=/home/ubuntu/cerebro/kvsdata
```

> **AWS Academy note:** stopping/starting the lab instances re-assigns their **public** IPs
> (private IPs persist; data on the EBS root volume persists). When that happens, update
> `COORD` and `WORKERS` in `.env` and run `./deploy-ec2.sh upgrade`. Get a host's private IP
> with `ssh … hostname -I`. Security groups must allow TCP 8000, 8001, 9000, 9001-9004 between
> the instances (and port 80 from the world for the frontend).

### 2. Commands

```
./deploy-ec2.sh deploy          # FRESH: wipe KVS data, deploy code, start services, submit the crawler
./deploy-ec2.sh upgrade         # re-upload + recompile code, restart KVS+Flame, KEEP all KVS data
                                #   (does NOT re-crawl unless CRAWL_ON_UPGRADE=1; does NOT start the frontend)
./deploy-ec2.sh run-indexer     # submit the Flame Indexer  -> builds pt-index
./deploy-ec2.sh run-pagerank    # submit the Flame PageRank -> builds pt-pageranks
./deploy-ec2.sh run-frontend    # start the frontend on :8080 + the port-80 redirect
./deploy-ec2.sh logs            # tail coordinator logs
./deploy-ec2.sh stats           # per-worker + total pt-crawl row counts
./deploy-ec2.sh stop            # stop all java on all hosts
```

Typical flow once the crawl is done:
`./deploy-ec2.sh upgrade` → `./deploy-ec2.sh run-frontend` → `./deploy-ec2.sh run-indexer`
(wait for it) → `./deploy-ec2.sh run-pagerank` (wait) → visit `http://<COORD>/`.

Flame jobs (`run-indexer`/`run-pagerank`) launch a detached `FlameSubmit` on the coordinator;
they keep running independent of your laptop. Only an expired AWS lab session or `stop` ends
them. Watch progress with `./deploy-ec2.sh logs`; errors land in
`~/cerebro/logs/{indexer,pagerank,flame-coord}.log` on the coordinator.

### 3. Tuning knobs

Environment variables read by `deploy-ec2.sh` (defaults shown are tuned for t3.medium = ~3.8 GB
RAM, 2 vCPU; raise them for bigger instances/corpora):

```
FLAME_CONCURRENCY=4            # threads per Flame worker (more concurrent KVS writes)
KVS_REPLICA_CONCURRENCY=1      # replica-forwarder threads per KVS worker
WEBSERVER_NUM_WORKERS=200      # webserver request-handler pool size
# e.g.:  FLAME_CONCURRENCY=8 ./deploy-ec2.sh upgrade
```

Indexer caps (in `Indexer.java`): `MAX_PAGE_CHARS`, `MAX_WORDS_PER_PAGE` — bound the work per
page (one KVS write per indexed word per page is the dominant cost). PageRank caps (in
`PageRank.java`): `MAX_LINKS_PER_PAGE`, `MAX_ITERATIONS` (each iteration is a full Flame
shuffle). Lower these for faster runs on a small cluster; raise them for a richer index /
better-converged PageRank on a bigger one. KVS/Flame worker heap sizes are also set in
`deploy-ec2.sh` (`-Xmx2g` for KVS, `-Xmx1g` for Flame) — bump them on larger instances.

## Crawl diagnostics

If crawl throughput spikes then collapses, run with `-Dcerebro.diag=true` (or `CEREBRO_DIAG=true`)
on the crawler + Flame processes and collect logs from every process over the same wall-clock
window (Flame coordinator, all Flame workers, KVS coordinator, all KVS workers, the crawler
submit stream) for 5-15 min — don't truncate around the slowdown.
