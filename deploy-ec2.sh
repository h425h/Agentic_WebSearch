#!/usr/bin/env bash
# Deploy + run the Cerebro crawler on 5 EC2 hosts
# (1 coordinator + 4 workers).
#
# Topology (mirrors run.bat, scaled across hosts):
#   Coordinator host: KVS Coordinator (8000) + Flame Coordinator (9000) + job submitter
#   Each Worker host: 1x KVS Worker (8001) + 1x Flame Worker (unique port 9001..9004)
#
# Usage:
#   cp .env.example .env && edit .env with your EC2 details
#   ./deploy-ec2.sh            # full deploy + crawler run
#   ./deploy-ec2.sh upgrade    # upload new code, preserve KVS data, restart crawler
#   ./deploy-ec2.sh resume     # restart services + crawler, preserve KVS data
#   ./deploy-ec2.sh stop       # stop all java processes
#   ./deploy-ec2.sh logs       # tail coordinator logs
#   ./deploy-ec2.sh stats      # per-worker + total pt-crawl counts
#   ./deploy-ec2.sh tunnel     # localhost tunnels to KVS workers only
#   ./deploy-ec2.sh pps        # pages/sec for each minute window
#   ./deploy-ec2.sh addkey ~/path/to/teammate.pub
#                                                    # install a teammate public key on every host
#   ./deploy-ec2.sh wipe-nvme  # DESTRUCTIVE: reformat the local NVMe instance
#                              # store on every worker (clears all KVS data)
#   ./deploy-ec2.sh backup-s3 <bucket>[/prefix]
#                              # tar+gzip every worker's NVMe data and stream
#                              # it to s3://<bucket>/cerebro-crawl/<timestamp>/
#   ./deploy-ec2.sh restore-s3 <bucket> <timestamp>
#                              # download a backup snapshot back onto the workers
#
# Security-group requirement: TCP 8000, 8001, 9000, and Flame worker ports
# 9001..9004 must be open between all instances
# (and to your laptop if you want to hit the web UIs).

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
ENV_FILE="${ENV_FILE:-$SCRIPT_DIR/.env}"
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

# Sensitive deployment values are loaded from .env.
COORD="${COORD:-}"
COORD_PRIVATE="${COORD_PRIVATE:-}"
WORKERS_RAW="${WORKERS:-}"
WORKERS_RAW="${WORKERS_RAW//,/ }"
read -r -a WORKERS <<<"$WORKERS_RAW"
DEPLOY_WORKER_COUNT=4
DEPLOY_WORKERS=("${WORKERS[@]:0:$DEPLOY_WORKER_COUNT}")

USER="${USER:-ubuntu}"
KVS_COORD_PORT=8000
FLAME_COORD_PORT=9000
KVS_WORKER_PORT=8001
FLAME_WORKER_BASE_PORT=9001
REMOTE_DIR="${REMOTE_DIR:-~/cerebro}"
NVME_MOUNT="${NVME_MOUNT:-/mnt/nvme}"
LOCAL_CRAWL_DIR="${LOCAL_CRAWL_DIR:-$HOME/Downloads/crawl}"
FRONTEND_PORT="${FRONTEND_PORT:-8080}"

CRAWLER_BLACKLIST_TABLE="${CRAWLER_BLACKLIST_TABLE:-pt-blacklist}"
# Wikipedia-heavy + a few diverse, crawler-friendly entry points: huge, well-interlinked, broad
# topic coverage, robots.txt that allows article paths. Override with CRAWLER_SEEDS env if needed.
CRAWLER_SEEDS=(
  https://en.wikipedia.org/wiki/Main_Page
  https://en.wikipedia.org/wiki/Portal:Contents
  https://en.wikipedia.org/wiki/Computer_science
  https://en.wikipedia.org/wiki/History_of_science
  https://en.wikipedia.org/wiki/Music
  https://en.wikipedia.org/wiki/Sports
  https://en.wikipedia.org/wiki/Geography
  https://simple.wikipedia.org/wiki/Main_Page
  https://www.britannica.com
  https://www.mit.edu
  https://blogroll.org
  https://techcrunch.com
)
if [[ -n "${CRAWLER_SEEDS_OVERRIDE:-}" ]]; then
  read -r -a CRAWLER_SEEDS <<<"$CRAWLER_SEEDS_OVERRIDE"
fi

SSH_KEY="${SSH_KEY:-}"
SSH_OPTS=(
  -i "$SSH_KEY"
  -o BatchMode=yes
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
  -o LogLevel=ERROR
  -o ConnectTimeout=10
  -o ConnectionAttempts=1
  -o ServerAliveInterval=5
  -o ServerAliveCountMax=2
)

ssh_run()  { ssh "${SSH_OPTS[@]}" "$USER@$1" "$2"; }
ssh_bg() {
  # Fully detach remote launch so ssh cannot block waiting on child process state.
  ssh "${SSH_OPTS[@]}" -n "$USER@$1" "nohup bash -lc \"$2\" >/dev/null 2>&1 < /dev/null &" </dev/null
}
scp_to()   { scp "${SSH_OPTS[@]}" -r "$1" "$USER@$2:$3"; }

all_hosts() { echo "$COORD" "${WORKERS[@]}"; }
deploy_hosts() { echo "$COORD" "${DEPLOY_WORKERS[@]}"; }

validate_config() {
  local missing=()
  [[ -z "$COORD" ]] && missing+=("COORD")
  [[ -z "$COORD_PRIVATE" ]] && missing+=("COORD_PRIVATE")
  [[ -z "$SSH_KEY" ]] && missing+=("SSH_KEY")
  (( ${#WORKERS[@]} == 0 )) && missing+=("WORKERS")

  if (( ${#missing[@]} > 0 )); then
    echo "ERROR: missing required config: ${missing[*]}"
    echo "Create $SCRIPT_DIR/.env from .env.example and set all values."
    exit 1
  fi

  if [[ ! -f "$SSH_KEY" ]]; then
    echo "ERROR: SSH key file not found: $SSH_KEY"
    exit 1
  fi

  if (( ${#WORKERS[@]} < DEPLOY_WORKER_COUNT )); then
    echo "ERROR: expected at least $DEPLOY_WORKER_COUNT WORKERS entries; found ${#WORKERS[@]}."
    exit 1
  fi
}

cmd_stop() {
  for h in $(all_hosts); do
    echo "[$h] killing java"
    ssh_run "$h" "pkill -9 -f cis5550 || true" &
  done
  wait
}

# Idempotent: format + mount the local NVMe instance store once per host.
# Safe to re-run -- mkfs is gated on `blkid`, mount is gated on `mountpoint -q`.
# i4i.large exposes a 468 GB local NVMe (typically /dev/nvme1n1); root EBS is
# /dev/nvme0n1. We auto-pick the largest unmounted disk so the script also
# works on other NVMe-backed instance families without code changes.
prepare_nvme() {
  local host="$1"
  ssh_run "$host" "NVME_MOUNT='$NVME_MOUNT' bash -s" <<'REMOTE'
set -euo pipefail
# Use EBS if no spare NVMe disk is available (e.g. t3.medium instances)
DEV=$(lsblk -dn -o NAME,SIZE,TYPE,MOUNTPOINT -b \
        | awk '$3=="disk" && $4=="" {print "/dev/"$1, $2}' \
        | sort -k2 -n -r | head -n1 | awk '{print $1}')

if [[ -n "${DEV:-}" ]]; then
  if ! sudo blkid "$DEV" >/dev/null 2>&1; then
    echo "[nvme] formatting fresh device $DEV as ext4"
    sudo mkfs.ext4 -F -L cerebro-nvme "$DEV"
  fi
  sudo mkdir -p "$NVME_MOUNT"
  # Only mount the fresh NVMe over NVME_MOUNT if it is currently EMPTY. If it
  # already holds data (e.g. KVS tables sitting on the EBS root volume after an
  # instance-type change t3.medium -> i4i, where the local NVMe is brand new),
  # do NOT shadow it -- keep serving the existing data off EBS.
  if ! mountpoint -q "$NVME_MOUNT" && [ -z "$(ls -A "$NVME_MOUNT" 2>/dev/null)" ]; then
    sudo mount -o noatime,nodiratime "$DEV" "$NVME_MOUNT"
  fi
  sudo chown ubuntu:ubuntu "$NVME_MOUNT" 2>/dev/null || true
else
  echo "[storage] no spare NVMe found — using EBS at $NVME_MOUNT"
  mkdir -p "$NVME_MOUNT"
fi

df -h "$NVME_MOUNT"
REMOTE
}

start_services() {
  echo "=== Starting KVS Coordinator on $COORD ==="
  ssh_bg "$COORD" "cd $REMOTE_DIR && mkdir -p logs && nohup java -Xmx2g -Xms1g -cp 'out:lib/*' cis5550.kvs.Coordinator $KVS_COORD_PORT >logs/kvs-coord.log 2>&1"
  sleep 3

  echo "=== Starting Flame Coordinator on $COORD ==="
  ssh_bg "$COORD" "cd $REMOTE_DIR && mkdir -p logs && nohup java -Xmx4g -Xms1g -cp 'out:lib/*' cis5550.flame.Coordinator $FLAME_COORD_PORT $COORD_PRIVATE:$KVS_COORD_PORT >logs/flame-coord.log 2>&1"
  sleep 3

  echo "=== Preparing NVMe instance store on workers ==="
  for w in "${DEPLOY_WORKERS[@]}"; do
    ( prepare_nvme "$w" && echo "[$w] nvme ready at $NVME_MOUNT" ) &
  done
  wait

  echo "=== Starting workers ==="
  # i4.large = 2 vCPU, 16 GB RAM, NVMe. Each host runs KVS+Flame:
  #   KVS worker  -Xmx8g : holds crawled pages; throughput grows fast.
  #                        Data lives on the local NVMe instance store at
  #                        $NVME_MOUNT/worker$i, not on EBS.
  #                        Replica forwarder pool sized via KVS_REPLICA_CONCURRENCY
  #                        (default 32) so PUT bursts don't fill the bounded queue
  #                        and trigger DiscardOldest drops. Anti-entropy repairs
  #                        any drops within ~30s.
  #   Flame worker -Xmx4g : 64 concurrent fetches * up to 5 MB MAX_CONTENT_SIZE
  #                         buffer ~= 320 MB peak, plus Loom carrier headroom.
  #   Both        : webserver request-handler pool sized via WEBSERVER_NUM_WORKERS
  #                 (default 100) -- the cap on concurrent inbound HTTP requests.
  # Leaves ~3 GB for OS + page cache.
  local FLAME_CONCURRENCY="${FLAME_CONCURRENCY:-4}"
  local KVS_REPLICA_CONCURRENCY="${KVS_REPLICA_CONCURRENCY:-1}"
  local WEBSERVER_NUM_WORKERS="${WEBSERVER_NUM_WORKERS:-200}"
  local i=1
  for w in "${DEPLOY_WORKERS[@]}"; do
    local flame_port=$((FLAME_WORKER_BASE_PORT + i - 1))
    ssh_bg "$w" "cd $REMOTE_DIR && mkdir -p $NVME_MOUNT/worker$i logs && nohup java -Xmx16g -Xms4g -Dkvs.worker.replicaConcurrency=$KVS_REPLICA_CONCURRENCY -Dwebserver.numWorkers=$WEBSERVER_NUM_WORKERS -cp 'out:lib/*' cis5550.kvs.Worker $KVS_WORKER_PORT $NVME_MOUNT/worker$i $COORD_PRIVATE:$KVS_COORD_PORT >logs/kvs-w$i.log 2>&1"
    ssh_bg "$w" "cd $REMOTE_DIR && mkdir -p logs && nohup java -Xmx6g -Xms1g -Dflame.worker.concurrency=$FLAME_CONCURRENCY -Dwebserver.numWorkers=$WEBSERVER_NUM_WORKERS -cp 'out:lib/*' cis5550.flame.Worker $flame_port $COORD_PRIVATE:$FLAME_COORD_PORT >logs/flame-w$i.log 2>&1"
    echo "[$w] started KVS+Flame #$i (flame_port=$flame_port, flame=$FLAME_CONCURRENCY, kvs_replica=$KVS_REPLICA_CONCURRENCY, webserver=$WEBSERVER_NUM_WORKERS)"
    i=$((i+1))
  done
}

wait_for_workers() {
  echo "=== Waiting for KVS + Flame workers to register ==="
  for try in $(seq 1 30); do
    local nk nf
    nk=$(ssh_run "$COORD" "curl -s http://localhost:$KVS_COORD_PORT/workers   | head -n1 | tr -d '\r\n '" || echo 0)
    nf=$(ssh_run "$COORD" "curl -s http://localhost:$FLAME_COORD_PORT/workers | head -n1 | tr -d '\r\n '" || echo 0)
    echo "  kvs: $nk   flame: $nf"
    if [[ "$nk" =~ ^[0-9]+$ ]] && [[ "$nf" =~ ^[0-9]+$ ]] \
        && (( nk >= ${#DEPLOY_WORKERS[@]} )) && (( nf >= ${#DEPLOY_WORKERS[@]} )); then
      return 0
    fi
    sleep 2
  done

  echo "ERROR: workers did not register in time"
  return 1
}

submit_crawler() {
  echo "=== Loading blacklist into KVS ==="
  ssh_run "$COORD" "cd $REMOTE_DIR && java -Xmx350m -Xms100m -cp 'out:lib/*' cis5550.tools.ConfigLoader localhost:$KVS_COORD_PORT config/blacklist.txt $CRAWLER_BLACKLIST_TABLE pattern" || echo "WARN: blacklist load failed"

  echo "=== Submitting crawler job ==="
  echo "  seeds           : ${CRAWLER_SEEDS[*]}"
  echo "  blacklist table : $CRAWLER_BLACKLIST_TABLE"
  ssh_bg "$COORD" "cd $REMOTE_DIR && nohup java -Xmx350m -Xms100m -cp 'out:lib/*' cis5550.flame.FlameSubmit localhost:$FLAME_COORD_PORT lib/crawler.jar cis5550.crawler.Crawler ${CRAWLER_SEEDS[*]} $CRAWLER_BLACKLIST_TABLE >logs/submit.log 2>&1"
}

cmd_logs() {
  ssh_run "$COORD" "tail -n 200 -F $REMOTE_DIR/logs/*.log"
}

cmd_stats() {
  ssh_run "$COORD" "KVS_COORD_PORT=$KVS_COORD_PORT bash -s" <<'EOF'
set -euo pipefail

workers_raw=$(curl -fsS "http://localhost:${KVS_COORD_PORT}/workers" || true)
if [[ -z "$workers_raw" ]]; then
  echo "ERROR: could not read KVS worker list from coordinator"
  exit 1
fi

declared_n=$(printf '%s\n' "$workers_raw" | head -n1 | tr -d '\r')
if ! [[ "$declared_n" =~ ^[0-9]+$ ]]; then
  echo "ERROR: unexpected /workers response"
  printf '%s\n' "$workers_raw"
  exit 1
fi

declare -a ids addrs
while IFS= read -r line; do
  [[ -z "${line//[[:space:]]/}" ]] && continue
  IFS=',' read -r id addr <<< "$line"
  [[ -z "$id" || -z "$addr" ]] && continue
  ids+=("$id")
  addrs+=("$addr")
done < <(printf '%s\n' "$workers_raw" | tail -n +2)

n=${#ids[@]}
if (( n == 0 )); then
  echo "No active KVS workers"
  exit 1
fi

count_range() {
  local addr="$1"
  local start="${2:-}"
  local end="${3:-}"
  local url="http://${addr}/data/pt-crawl?keyonly=true"

  if [[ -n "$start" ]]; then
    url+="&startRow=$start"
  fi
  if [[ -n "$end" ]]; then
    url+="&endRowExclusive=$end"
  fi

  local resp body code
  resp=$(curl -sS -w $'\n%{http_code}' "$url" 2>/dev/null || true)
  code=$(printf '%s\n' "$resp" | tail -n1)
  body=$(printf '%s\n' "$resp" | sed '$d')

  # 404 means this worker has no local pt-crawl table for this range.
  if [[ "$code" == "404" || -z "$body" ]]; then
    echo 0
    return
  fi

  if [[ "$code" != "200" ]]; then
    echo 0
    return
  fi

  printf '%s\n' "$body" | awk 'NF{c++} END{print c+0}'
}

echo "=== pt-crawl primary-row counts by KVS worker ==="
overall=0
for ((i=0; i<n; i++)); do
  this_id="${ids[$i]}"
  this_addr="${addrs[$i]}"
  if (( i < n-1 )); then
    next_id="${ids[$((i+1))]}"
    c=$(count_range "$this_addr" "$this_id" "$next_id")
  else
    first_id="${ids[0]}"
    c1=$(count_range "$this_addr" "$this_id" "")
    c2=$(count_range "$this_addr" "" "$first_id")
    c=$((c1 + c2))
  fi
  overall=$((overall + c))
  printf '%-8s %-21s %10d\n' "$this_id" "$this_addr" "$c"
done

echo
echo "Overall pt-crawl row count: $overall"
EOF
}

get_pt_crawl_total() {
  ssh_run "$COORD" "KVS_COORD_PORT=$KVS_COORD_PORT bash -s" <<'EOF'
set -euo pipefail

workers_raw=$(curl -fsS "http://localhost:${KVS_COORD_PORT}/workers" || true)
if [[ -z "$workers_raw" ]]; then
  echo 0
  exit 0
fi

declare -a ids addrs
while IFS= read -r line; do
  [[ -z "${line//[[:space:]]/}" ]] && continue
  IFS=',' read -r id addr <<< "$line"
  [[ -z "$id" || -z "$addr" ]] && continue
  ids+=("$id")
  addrs+=("$addr")
done < <(printf '%s\n' "$workers_raw" | tail -n +2)

n=${#ids[@]}
if (( n == 0 )); then
  echo 0
  exit 0
fi

count_range() {
  local addr="$1"
  local start="${2:-}"
  local end="${3:-}"
  local url="http://${addr}/data/pt-crawl?keyonly=true"

  if [[ -n "$start" ]]; then
    url+="&startRow=$start"
  fi
  if [[ -n "$end" ]]; then
    url+="&endRowExclusive=$end"
  fi

  local resp body code
  resp=$(curl -sS -w $'\n%{http_code}' "$url" 2>/dev/null || true)
  code=$(printf '%s\n' "$resp" | tail -n1)
  body=$(printf '%s\n' "$resp" | sed '$d')

  if [[ "$code" == "404" || -z "$body" ]]; then
    echo 0
    return
  fi
  if [[ "$code" != "200" ]]; then
    echo 0
    return
  fi

  printf '%s\n' "$body" | awk 'NF{c++} END{print c+0}'
}

overall=0
for ((i=0; i<n; i++)); do
  this_id="${ids[$i]}"
  this_addr="${addrs[$i]}"
  if (( i < n-1 )); then
    next_id="${ids[$((i+1))]}"
    c=$(count_range "$this_addr" "$this_id" "$next_id")
  else
    first_id="${ids[0]}"
    c1=$(count_range "$this_addr" "$this_id" "")
    c2=$(count_range "$this_addr" "" "$first_id")
    c=$((c1 + c2))
  fi
  overall=$((overall + c))
done

echo "$overall"
EOF
}

cmd_pps() {
  local interval=60
  local prev_count prev_ts
  prev_count=$(get_pt_crawl_total)
  prev_ts=$(date +%s)

  if ! [[ "$prev_count" =~ ^[0-9]+$ ]]; then
    echo "ERROR: could not read initial pt-crawl count"
    exit 1
  fi

  echo "Tracking pt-crawl throughput every ${interval}s. Press Ctrl+C to stop."
  echo "window_end_utc,total_count,rows_in_window,pages_per_second"

  while true; do
    sleep "$interval"
    local now_count now_ts delta_count delta_ts pps ts
    now_count=$(get_pt_crawl_total)
    now_ts=$(date +%s)

    if ! [[ "$now_count" =~ ^[0-9]+$ ]]; then
      now_count=$prev_count
    fi

    delta_count=$((now_count - prev_count))
    delta_ts=$((now_ts - prev_ts))
    if (( delta_count < 0 )); then
      delta_count=0
    fi
    if (( delta_ts <= 0 )); then
      delta_ts=$interval
    fi

    pps=$(awk -v d="$delta_count" -v t="$delta_ts" 'BEGIN { printf "%.3f", (t>0 ? d/t : 0) }')
    ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "$ts,$now_count,$delta_count,$pps"

    prev_count=$now_count
    prev_ts=$now_ts
  done
}

cmd_tunnel() {
  local kvs_raw
  kvs_raw=$(ssh_run "$COORD" "curl -fsS http://localhost:$KVS_COORD_PORT/workers" || true)

  if [[ -z "$kvs_raw" ]]; then
    echo "ERROR: could not read KVS worker list from coordinator"
    exit 1
  fi

  local -a forwards
  local -a local_urls
  forwards=()
  local_urls=()

  echo "=== Tunnel mapping ==="

  local i=1
  while IFS= read -r line; do
    [[ -z "${line//[[:space:]]/}" ]] && continue
    [[ "$line" =~ ^[0-9]+$ ]] && continue
    local id addr host port lport
    IFS=',' read -r id addr <<< "$line"
    host=${addr%:*}
    port=${addr##*:}
    lport=$((8100 + i))
    forwards+=( -L "$lport:$host:$port" )
    local_urls+=("http://localhost:$lport/")
    echo "kvs-worker  localhost:$lport -> $host:$port (id $id)"
    i=$((i+1))
  done < <(printf '%s\n' "$kvs_raw")

  if (( ${#forwards[@]} == 0 )); then
    echo "ERROR: no active KVS workers found to tunnel"
    exit 1
  fi

  echo
  echo "Tunnel will stay open until you press Ctrl+C."
  echo "Open in browser:"
  for url in "${local_urls[@]}"; do
    echo "  $url"
  done

  ssh "${SSH_OPTS[@]}" -o ExitOnForwardFailure=yes -N "${forwards[@]}" "$USER@$COORD"
}

cmd_addkey() {
  local pubkey_file="${2:-${PUBKEY_FILE:-}}"
  local pubkey key_b64

  if [[ -z "$pubkey_file" ]]; then
    echo "usage: $0 addkey /path/to/teammate.pub"
    exit 1
  fi

  if [[ ! -f "$pubkey_file" ]]; then
    echo "ERROR: public key file not found: $pubkey_file"
    exit 1
  fi

  pubkey=$(tr -d '\r' < "$pubkey_file")
  if [[ -z "${pubkey//[[:space:]]/}" ]]; then
    echo "ERROR: public key file is empty: $pubkey_file"
    exit 1
  fi

  case "$pubkey" in
    ssh-rsa\ *|ssh-ed25519\ *|ecdsa-*\ *)
      ;;
    *)
      echo "ERROR: unexpected public key format in $pubkey_file"
      exit 1
      ;;
  esac

  key_b64=$(printf '%s' "$pubkey" | base64 | tr -d '\n')

  echo "=== Installing public key from $pubkey_file on all hosts ==="
  for h in $(all_hosts); do
    (
      ssh_run "$h" "KEY_B64='$key_b64' bash -lc 'set -euo pipefail; key=\$(printf %s \"\$KEY_B64\" | base64 -d); mkdir -p ~/.ssh; chmod 700 ~/.ssh; touch ~/.ssh/authorized_keys; chmod 600 ~/.ssh/authorized_keys; grep -qxF \"\$key\" ~/.ssh/authorized_keys || printf \"%s\n\" \"\$key\" >> ~/.ssh/authorized_keys'"
      echo "[$h] key installed"
    ) &
  done
  wait

  cat <<EOF

=== Done ===
Installed key from: $pubkey_file
Hosts             : $(all_hosts)
EOF
}

compile_all_hosts() {
  echo "=== Compiling and building jars on deployment hosts ==="
  local COMPILE
  COMPILE='cd '"$REMOTE_DIR"' && rm -rf out && mkdir -p out lib && find lib -name "._*" -delete'
  COMPILE+=' && javac -cp "lib/*" -d out'
  COMPILE+=' src/cis5550/webserver/*.java'
  COMPILE+=' src/cis5550/generic/*.java'
  COMPILE+=' src/cis5550/kvs/*.java'
  COMPILE+=' src/cis5550/tools/*.java'
  COMPILE+=' src/cis5550/external/*.java'
  COMPILE+=' src/cis5550/flame/*.java'
  COMPILE+=' src/cis5550/crawler/Crawler.java'
  COMPILE+=' src/cis5550/indexer/Indexer.java'
  COMPILE+=' src/cis5550/pagerank/PageRank.java'
  COMPILE+=' src/cis5550/frontend/Frontend.java'
  COMPILE+=' src/cis5550/frontend/SeedIndex.java'
  COMPILE+=' && rm -f lib/crawler.jar lib/indexer.jar'
  COMPILE+=' && jar cf lib/crawler.jar -C out cis5550/crawler -C out cis5550/tools'
  COMPILE+=' && jar cf lib/indexer.jar -C out cis5550/jobs -C out cis5550/external -C out cis5550/tools'
  for h in $(deploy_hosts); do
    ( ssh_run "$h" "$COMPILE" && echo "[$h] compiled" ) &
  done
  wait
}

cmd_deploy() {
  # DESTRUCTIVE: this wipes ALL KVS data (pt-crawl, pt-index, pt-pageranks) on every worker.
  # Use 'upgrade' to re-deploy code while keeping the data. Require an explicit confirmation.
  if [[ "${CONFIRM_WIPE:-}" != "yes" ]]; then
    echo "!!! 'deploy' WIPES ALL KVS DATA on every worker (pt-crawl / pt-index / pt-pageranks)."
    echo "    To re-deploy code WITHOUT wiping data, use:  $0 upgrade"
    read -r -p "    Type 'yes' to confirm a destructive fresh deploy: " ans
    [[ "$ans" == "yes" ]] || { echo "Aborted."; exit 1; }
  fi
  echo "=== Killing any running java from a previous deploy ==="
  for h in $(all_hosts); do
    ssh_run "$h" "pkill -9 -f cis5550 || true" &
  done
  wait

  echo "=== Packaging source ==="
  local TMP
  local SCRIPT_DIR
  TMP=$(mktemp -d)
  SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
  local -a package_items=(src config)
  if [[ -d "$SCRIPT_DIR/lib" ]]; then
    package_items+=(lib)
  fi
  if [[ -d "$SCRIPT_DIR/static" ]]; then
    package_items+=(static)
  fi
  tar czf "$TMP/cerebro.tgz" -C "$SCRIPT_DIR" "${package_items[@]}"

  echo "=== Installing JDK + uploading source to deployment hosts ==="
  for h in $(deploy_hosts); do
    (
      ssh_run "$h" "(javac -version 2>&1 | grep -q 'javac 21' || (sudo apt-get update -qq && sudo apt-get install -y -qq openjdk-21-jdk-headless >/dev/null && sudo update-alternatives --set java /usr/lib/jvm/java-21-openjdk-amd64/bin/java && sudo update-alternatives --set javac /usr/lib/jvm/java-21-openjdk-amd64/bin/javac)) && mkdir -p $REMOTE_DIR && rm -rf $REMOTE_DIR/out $REMOTE_DIR/src $REMOTE_DIR/config $REMOTE_DIR/logs && mkdir -p $REMOTE_DIR/logs"
      scp_to "$TMP/cerebro.tgz" "$h" "$REMOTE_DIR/cerebro.tgz"
      ssh_run "$h" "cd $REMOTE_DIR && tar xzf cerebro.tgz && rm cerebro.tgz"
    ) &
  done
  wait
  rm -rf "$TMP"

  compile_all_hosts

  # Fresh deploy: ensure NVMe is mounted, then clear any leftover worker dirs
  # from a previous run so we start with a clean KVS state. Only done here --
  # `upgrade` and `resume` deliberately preserve existing NVMe data.
  echo "=== Wiping NVMe worker dirs for fresh deploy ==="
  for w in "${DEPLOY_WORKERS[@]}"; do
    (
      prepare_nvme "$w"
      ssh_run "$w" "sudo rm -rf $NVME_MOUNT/worker* && echo '[$w] cleared $NVME_MOUNT/worker*'"
    ) &
  done
  wait

  start_services
  wait_for_workers
  submit_crawler

  cat <<EOF

=== Done ===
KVS Coord   : http://$COORD:$KVS_COORD_PORT/
Flame Coord : http://$COORD:$FLAME_COORD_PORT/
Tail logs   : $0 logs
Watch pps   : $0 pps
Stop all    : $0 stop
EOF
}

cmd_upgrade() {
  echo "=== Uploading new code without deleting KVS worker data ==="
  cmd_stop

  echo "=== Packaging source ==="
  local TMP
  local SCRIPT_DIR
  TMP=$(mktemp -d)
  SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
  local -a package_items=(src config)
  if [[ -d "$SCRIPT_DIR/lib" ]]; then
    package_items+=(lib)
  fi
  if [[ -d "$SCRIPT_DIR/static" ]]; then
    package_items+=(static)
  fi
  tar czf "$TMP/cerebro.tgz" -C "$SCRIPT_DIR" "${package_items[@]}"

  echo "=== Installing JDK + uploading source to deployment hosts ==="
  for h in $(deploy_hosts); do
    (
      ssh_run "$h" "(javac -version 2>&1 | grep -q 'javac 21' || (sudo apt-get update -qq && sudo apt-get install -y -qq openjdk-21-jdk-headless >/dev/null && sudo update-alternatives --set java /usr/lib/jvm/java-21-openjdk-amd64/bin/java && sudo update-alternatives --set javac /usr/lib/jvm/java-21-openjdk-amd64/bin/javac)) && mkdir -p $REMOTE_DIR && rm -rf $REMOTE_DIR/out $REMOTE_DIR/src $REMOTE_DIR/config && mkdir -p $REMOTE_DIR/logs"
      scp_to "$TMP/cerebro.tgz" "$h" "$REMOTE_DIR/cerebro.tgz"
      ssh_run "$h" "cd $REMOTE_DIR && tar xzf cerebro.tgz && rm cerebro.tgz"
    ) &
  done
  wait
  rm -rf "$TMP"

  compile_all_hosts

  start_services
  wait_for_workers
  # Crawl phase is normally done by the time you upgrade; re-crawling would
  # compete with the indexer/pagerank jobs for KVS capacity. Opt in explicitly.
  if [[ "${CRAWL_ON_UPGRADE:-0}" == "1" ]]; then
    submit_crawler
  else
    echo "=== Skipping crawler re-submission (set CRAWL_ON_UPGRADE=1 to re-crawl) ==="
  fi

  cat <<EOF

=== Upgrade complete ===
KVS Coord   : http://$COORD:$KVS_COORD_PORT/
Flame Coord : http://$COORD:$FLAME_COORD_PORT/
Next        : $0 run-indexer  ->  $0 run-pagerank  ->  $0 run-frontend
Tail logs   : $0 logs
Stop all    : $0 stop
EOF
}

cmd_resume() {
  echo "=== Restarting services without deleting KVS worker data ==="
  cmd_stop

  for h in $(deploy_hosts); do
    if ! ssh_run "$h" "test -d $REMOTE_DIR/out && test -f $REMOTE_DIR/lib/crawler.jar" >/dev/null; then
      echo "ERROR: $h does not have a deployed build. Run $0 deploy first."
      exit 1
    fi
  done

  start_services
  wait_for_workers
  submit_crawler

  cat <<EOF

=== Resume Submitted ===
KVS Coord   : http://$COORD:$KVS_COORD_PORT/
Flame Coord : http://$COORD:$FLAME_COORD_PORT/
Tail logs   : $0 logs
Watch pps   : $0 pps
Stop all    : $0 stop
EOF
}

# Destructive: unmount and reformat the local NVMe instance store on every
# worker. All KVS data on the NVMe is lost. Requires an explicit user action.
cmd_wipe_nvme() {
  echo "=== Wiping NVMe instance store on workers ==="
  cmd_stop
  for w in "${DEPLOY_WORKERS[@]}"; do
    (
      ssh_run "$w" "NVME_MOUNT='$NVME_MOUNT' bash -s" <<'REMOTE'
set -euo pipefail
sudo umount "$NVME_MOUNT" 2>/dev/null || true
DEV=$(lsblk -dn -o NAME,SIZE,TYPE,MOUNTPOINT -b \
        | awk '$3=="disk" && $4=="" {print "/dev/"$1, $2}' \
        | sort -k2 -n -r | head -n1 | awk '{print $1}')
if [[ -z "${DEV:-}" ]]; then
  echo "ERROR: no unmounted NVMe disk found" >&2
  lsblk >&2
  exit 1
fi
echo "[nvme] reformatting $DEV as ext4"
sudo mkfs.ext4 -F -L cerebro-nvme "$DEV"
REMOTE
      echo "[$w] nvme wiped"
    ) &
  done
  wait
}

# Build the env-var prefix that ships AWS credentials + region to a remote shell.
# Workers without an instance profile need AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
# in .env; workers WITH an instance profile can leave them unset.
aws_env_prefix() {
  local prefix=""
  if [[ -n "${AWS_ACCESS_KEY_ID:-}" && -n "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
    prefix="AWS_ACCESS_KEY_ID='${AWS_ACCESS_KEY_ID}' AWS_SECRET_ACCESS_KEY='${AWS_SECRET_ACCESS_KEY}'"
    if [[ -n "${AWS_SESSION_TOKEN:-}" ]]; then
      prefix+=" AWS_SESSION_TOKEN='${AWS_SESSION_TOKEN}'"
    fi
  fi
  local region="${AWS_DEFAULT_REGION:-${AWS_REGION:-us-east-1}}"
  prefix+=" AWS_DEFAULT_REGION='${region}'"
  printf '%s' "$prefix"
}

ensure_aws_cli() {
  local host="$1"
  ssh_run "$host" "command -v aws >/dev/null 2>&1 || (sudo apt-get update -qq && sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq awscli >/dev/null)"
}

# Long-running ssh -- detaches the remote command via nohup so transient ssh drops
# don't kill multi-minute uploads. Writes remote output to logs/<tag>.log on the
# worker and tails it locally until the marker file appears. Returns the remote
# exit code via a sentinel file.
ssh_long_run() {
  local host="$1" tag="$2" remote_cmd="$3"
  local marker="/tmp/cerebro-${tag}.done"
  local rclog="/tmp/cerebro-${tag}.rc"
  local log="/tmp/cerebro-${tag}.log"

  # Loosen keepalive for the launch/poll connections so the global aggressive
  # ServerAlive settings don't sever us during quiet upload windows.
  local LONG_OPTS=(
    -i "$SSH_KEY"
    -o BatchMode=yes
    -o StrictHostKeyChecking=no
    -o UserKnownHostsFile=/dev/null
    -o LogLevel=ERROR
    -o ConnectTimeout=10
    -o ServerAliveInterval=30
    -o ServerAliveCountMax=10
  )

  # Launch detached. The remote script writes rc and touches the marker on exit.
  ssh "${LONG_OPTS[@]}" -n "$USER@$host" \
    "rm -f '$marker' '$rclog' '$log'; nohup bash -lc '{ $remote_cmd ; } >$log 2>&1; echo \$? > $rclog; touch $marker' >/dev/null 2>&1 < /dev/null &" \
    </dev/null

  # Poll for completion every 5s, streaming new log lines as they appear.
  local last_size=0
  while true; do
    sleep 5
    # Stream any new log content
    local cur_size new
    cur_size=$(ssh "${LONG_OPTS[@]}" "$USER@$host" "stat -c %s '$log' 2>/dev/null || echo 0")
    if [[ "$cur_size" =~ ^[0-9]+$ ]] && (( cur_size > last_size )); then
      new=$(ssh "${LONG_OPTS[@]}" "$USER@$host" "tail -c +$((last_size+1)) '$log' 2>/dev/null || true")
      [[ -n "$new" ]] && printf '%s\n' "$new"
      last_size=$cur_size
    fi
    # Check if done
    if ssh "${LONG_OPTS[@]}" "$USER@$host" "test -f '$marker'" 2>/dev/null; then
      local rc
      rc=$(ssh "${LONG_OPTS[@]}" "$USER@$host" "cat '$rclog' 2>/dev/null || echo 1")
      [[ "$rc" =~ ^[0-9]+$ ]] || rc=1
      return "$rc"
    fi
  done
}

cmd_backup_s3() {
  local arg="${2:-${S3_BUCKET:-}}"
  if [[ -z "$arg" ]]; then
    echo "usage: $0 backup-s3 <s3-bucket>[/optional/prefix]"
    echo "  or set S3_BUCKET in .env"
    exit 1
  fi
  # Accept s3://bucket/path, bucket/path, or bucket
  arg="${arg#s3://}"
  arg="${arg%/}"
  local bucket="${arg%%/*}"
  local user_prefix=""
  if [[ "$arg" == */* ]]; then
    user_prefix="${arg#*/}/"
  fi

  local stamp
  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  local s3_base="s3://${bucket}/${user_prefix}cerebro-crawl/${stamp}"
  local aws_env
  aws_env="$(aws_env_prefix)"

  echo "=== Backing up KVS worker data to ${s3_base}/ ==="
  echo "    (each backup is a timestamped, independent snapshot)"

  local results_dir
  results_dir=$(mktemp -d)
  local i=1
  for w in "${DEPLOY_WORKERS[@]}"; do
    local src_dir="$NVME_MOUNT/worker$i"
    local dst="$s3_base/worker$i.tar.gz"
    local rc_file="$results_dir/worker$i.rc"
    local idx=$i
    (
      ensure_aws_cli "$w"
      remote_cmd="$aws_env bash -c '
set -euo pipefail
if [[ ! -d $src_dir ]]; then
  echo \"[worker$idx] $src_dir does not exist on \$(hostname) -- skipping\"
  exit 0
fi
size=\$(du -sh $src_dir 2>/dev/null | cut -f1)
echo \"[worker$idx] streaming \$size from $src_dir -> $dst\"
tar -C $NVME_MOUNT -cf - worker$idx | gzip -1 | aws s3 cp - $dst --no-progress
echo \"[worker$idx] done -> $dst\"
'"
      if ssh_long_run "$w" "backup-w$idx" "$remote_cmd"; then
        echo "0" > "$rc_file"
      else
        echo "$?" > "$rc_file"
        echo "[worker$idx] FAILED on $w (see /tmp/cerebro-backup-w$idx.log on host)"
      fi
    ) &
    i=$((i+1))
  done
  wait

  local failures=0 ok=0
  for f in "$results_dir"/*.rc; do
    if [[ "$(cat "$f")" == "0" ]]; then
      ok=$((ok+1))
    else
      failures=$((failures+1))
    fi
  done
  rm -rf "$results_dir"

  if (( failures > 0 )); then
    cat <<EOF

=== Backup INCOMPLETE: $ok/$((ok+failures)) workers succeeded ===
Failed workers logged their output to /tmp/cerebro-backup-wN.log on each host.
Re-run to retry; successful workers overwrite their tar.gz harmlessly.
S3 prefix : $s3_base/
EOF
    exit 1
  fi

  cat <<EOF

=== Backup complete: all $ok workers uploaded ===
S3 prefix    : $s3_base/
List backups : aws s3 ls s3://${bucket}/${user_prefix}cerebro-crawl/
Inspect      : aws s3 ls $s3_base/
Restore      : $0 restore-s3 ${bucket}${user_prefix:+/${user_prefix%/}} $stamp
EOF
}

cmd_restore_s3() {
  local arg="${2:-${S3_BUCKET:-}}"
  local stamp="${3:-}"
  if [[ -z "$arg" || -z "$stamp" ]]; then
    echo "usage: $0 restore-s3 <bucket>[/prefix] <timestamp>"
    echo "  list snapshots: aws s3 ls s3://<bucket>/<prefix>/cerebro-crawl/"
    exit 1
  fi
  arg="${arg#s3://}"
  arg="${arg%/}"
  local bucket="${arg%%/*}"
  local user_prefix=""
  if [[ "$arg" == */* ]]; then
    user_prefix="${arg#*/}/"
  fi
  local s3_base="s3://${bucket}/${user_prefix}cerebro-crawl/${stamp}"
  local aws_env
  aws_env="$(aws_env_prefix)"

  if [[ "${CONFIRM_RESTORE:-}" != "yes" ]]; then
    echo "!!! 'restore-s3' OVERWRITES the worker$i NVMe dirs on every worker."
    echo "    Source: $s3_base/"
    read -r -p "    Type 'yes' to proceed: " ans
    [[ "$ans" == "yes" ]] || { echo "Aborted."; exit 1; }
  fi

  cmd_stop

  echo "=== Preparing NVMe on workers ==="
  for w in "${DEPLOY_WORKERS[@]}"; do
    ( prepare_nvme "$w" ) &
  done
  wait

  local i=1
  for w in "${DEPLOY_WORKERS[@]}"; do
    local src="$s3_base/worker$i.tar.gz"
    (
      ensure_aws_cli "$w"
      ssh_run "$w" "$aws_env bash -lc '
set -euo pipefail
echo \"[worker$i] removing existing $NVME_MOUNT/worker$i\"
rm -rf $NVME_MOUNT/worker$i
echo \"[worker$i] downloading $src -> $NVME_MOUNT/worker$i\"
aws s3 cp $src - --no-progress | tar -C $NVME_MOUNT -xzf -
echo \"[worker$i] restore complete\"
'"
    ) &
    i=$((i+1))
  done
  wait

  cat <<EOF

=== Restore complete ===
Start services : $0 restart-services
EOF
}

cmd_load_data() {
  echo "=== load-data: upload existing crawl data and start all services ==="

  echo "--- Stopping any running Java on all hosts ---"
  for h in $(all_hosts); do
    ssh_run "$h" "pkill -9 -f cis5550 || true" &
  done
  wait

  echo "--- Packaging and uploading source to all deploy hosts ---"
  local TMP SCRIPT_DIR_LOCAL
  TMP=$(mktemp -d)
  SCRIPT_DIR_LOCAL=$(cd "$(dirname "$0")" && pwd)
  local -a package_items=(src config)
  [[ -d "$SCRIPT_DIR_LOCAL/lib" ]]    && package_items+=(lib)
  [[ -d "$SCRIPT_DIR_LOCAL/static" ]] && package_items+=(static)
  tar czf "$TMP/cerebro.tgz" -C "$SCRIPT_DIR_LOCAL" "${package_items[@]}"
  for h in $(deploy_hosts); do
    (
      ssh_run "$h" "(javac -version 2>&1 | grep -q 'javac 21' || (sudo apt-get update -qq && sudo apt-get install -y -qq openjdk-21-jdk-headless >/dev/null && sudo update-alternatives --set java /usr/lib/jvm/java-21-openjdk-amd64/bin/java && sudo update-alternatives --set javac /usr/lib/jvm/java-21-openjdk-amd64/bin/javac)) && mkdir -p $REMOTE_DIR && rm -rf $REMOTE_DIR/out $REMOTE_DIR/src $REMOTE_DIR/config && mkdir -p $REMOTE_DIR/logs"
      scp_to "$TMP/cerebro.tgz" "$h" "$REMOTE_DIR/cerebro.tgz"
      ssh_run "$h" "cd $REMOTE_DIR && tar xzf cerebro.tgz && rm cerebro.tgz"
    ) &
  done
  wait
  rm -rf "$TMP"

  compile_all_hosts

  echo "--- Uploading crawl data from $LOCAL_CRAWL_DIR to workers ---"
  local i=1
  for w in "${DEPLOY_WORKERS[@]}"; do
    local local_dir="$LOCAL_CRAWL_DIR/worker$i"
    if [[ ! -d "$local_dir" ]]; then
      echo "WARNING: $local_dir not found — skipping worker $i"
      i=$((i+1)); continue
    fi
    (
      ssh_run "$w" "mkdir -p $NVME_MOUNT"
      echo "  [worker$i → $w] uploading $(du -sh "$local_dir" 2>/dev/null | cut -f1) of data..."
      scp_to "$local_dir" "$w" "$NVME_MOUNT/"
      echo "  [worker$i → $w] upload complete"
    ) &
    i=$((i+1))
  done
  wait

  start_services
  wait_for_workers

  cat <<EOF

=== Ready ===
KVS Coord   : http://$COORD:$KVS_COORD_PORT/
Flame Coord : http://$COORD:$FLAME_COORD_PORT/

Next steps (run in order):
  $0 run-indexer     # build pt-index from pt-crawl (long job)
  $0 run-pagerank    # build pt-pageranks (long job)
  $0 run-frontend    # start the search UI on port $FRONTEND_PORT (+ redirect 80→$FRONTEND_PORT)
EOF
}

cmd_restart_services() {
  echo "=== Restarting KVS + Flame services (preserving all data) ==="
  cmd_stop

  # Recompile if out/ is empty (e.g. after instance stop/start wiped RAM-backed dirs)
  local needs_compile=false
  for h in $(deploy_hosts); do
    local class_count
    class_count=$(ssh_run "$h" "find $REMOTE_DIR/out -name '*.class' 2>/dev/null | wc -l" 2>/dev/null || echo 0)
    if [[ "$class_count" -eq 0 ]]; then
      needs_compile=true
      break
    fi
  done

  if $needs_compile; then
    echo "--- out/ is empty on one or more hosts, recompiling ---"
    compile_all_hosts
  fi

  start_services
  wait_for_workers
  cat <<EOF

=== Services running ===
KVS Coord   : http://$COORD:$KVS_COORD_PORT/
Flame Coord : http://$COORD:$FLAME_COORD_PORT/
Next: $0 run-indexer  /  $0 run-frontend
EOF
}

cmd_run_crawler() {
  # Re-submit the crawler against an already-deployed cluster (no re-deploy, no data wipe). If
  # pt-crawl already has pages, the crawler rebuilds its frontier from their out-links and resumes.
  # Run the Flame workers at a MODERATE concurrency for crawling — too high (~64) overwhelms the
  # hotspot KVS worker's accept backlog and stalls the crawl. ~16-32 is the sweet spot:
  #   FLAME_CONCURRENCY=24 ./deploy-ec2.sh upgrade   # (or restart-services), then:
  #   ./deploy-ec2.sh run-crawler
  submit_crawler
  cat <<EOF

=== Crawler submitted ===
  seeds : ${CRAWLER_SEEDS[*]}
  (if pt-crawl was non-empty, it's resuming from a rebuilt frontier, not these seeds)
  Tail logs   : $0 logs
  pps         : $0 pps
  pt-crawl    : $0 stats
  Stop crawler: ssh in and 'jps -l | awk "/FlameSubmit/{print \$1}" | xargs -r kill -9'  (then $0 restart-services)
EOF
}

cmd_run_indexer() {
  echo "=== Submitting Indexer Flame job ==="
  echo "Output: $REMOTE_DIR/logs/indexer.log on coordinator"
  ssh_bg "$COORD" "cd $REMOTE_DIR && nohup java -Xmx350m -Xms100m -cp 'out:lib/*' cis5550.flame.FlameSubmit localhost:$FLAME_COORD_PORT lib/indexer.jar cis5550.jobs.Indexer >logs/indexer.log 2>&1"
  echo "Submitted. Watch: $0 logs"
}

cmd_run_pagerank() {
  echo "=== Submitting PageRank Flame job ==="
  echo "Output: $REMOTE_DIR/logs/pagerank.log on coordinator"
  ssh_bg "$COORD" "cd $REMOTE_DIR && nohup java -Xmx350m -Xms100m -cp 'out:lib/*' cis5550.flame.FlameSubmit localhost:$FLAME_COORD_PORT lib/indexer.jar cis5550.jobs.PageRank >logs/pagerank.log 2>&1"
  echo "Submitted. Watch: $0 logs"
}

cmd_run_frontend() {
  echo "=== Starting Cerebro frontend on coordinator ==="
  # Kill any frontend by the port it listens on. (Do NOT use `pkill -f 'Frontend $PORT'`:
  # that pattern also matches the shell running the pkill, so it kills its own SSH session
  # — which made this command fail with exit 255.)
  ssh_run "$COORD" "sudo fuser -k ${FRONTEND_PORT}/tcp >/dev/null 2>&1 || true"
  # Redirect port 80 → FRONTEND_PORT so cerebro.cis5550.net works without a port number
  ssh_run "$COORD" "sudo iptables -t nat -C PREROUTING -p tcp --dport 80 -j REDIRECT --to-port $FRONTEND_PORT 2>/dev/null || sudo iptables -t nat -A PREROUTING -p tcp --dport 80 -j REDIRECT --to-port $FRONTEND_PORT || true"
  ssh_run "$COORD" "sudo iptables -t nat -C OUTPUT -p tcp -d 127.0.0.1 --dport 80 -j REDIRECT --to-port $FRONTEND_PORT 2>/dev/null || sudo iptables -t nat -A OUTPUT -p tcp -d 127.0.0.1 --dport 80 -j REDIRECT --to-port $FRONTEND_PORT || true"
  ssh_bg "$COORD" "cd $REMOTE_DIR && nohup java -Xmx1g -Xms256m -cp 'out:lib/*' cis5550.frontend.Frontend $FRONTEND_PORT localhost:$KVS_COORD_PORT >logs/frontend.log 2>&1"
  sleep 2
  cat <<EOF

=== Frontend running ===
  http://$COORD:$FRONTEND_PORT
  http://$COORD           (via iptables 80→$FRONTEND_PORT redirect)
  Tail logs : $0 logs
EOF
}

validate_config

case "${1:-deploy}" in
  deploy)        cmd_deploy ;;
  load-data)     cmd_load_data ;;
  upgrade)       cmd_upgrade ;;
  resume)        cmd_resume ;;
  stop)          cmd_stop ;;
  logs)          cmd_logs ;;
  stats)         cmd_stats ;;
  tunnel)        cmd_tunnel ;;
  pps)           cmd_pps ;;
  addkey)        cmd_addkey "$@" ;;
  wipe-nvme)     cmd_wipe_nvme ;;
  backup-s3)     cmd_backup_s3 "$@" ;;
  restore-s3)    cmd_restore_s3 "$@" ;;
  restart-services) cmd_restart_services ;;
  run-crawler)   cmd_run_crawler ;;
  run-indexer)   cmd_run_indexer ;;
  run-pagerank)  cmd_run_pagerank ;;
  run-frontend)  cmd_run_frontend ;;
  *) echo "usage: $0 [deploy|load-data|upgrade|resume|stop|logs|stats|tunnel|pps|addkey|wipe-nvme|backup-s3|restore-s3|restart-services|run-crawler|run-indexer|run-pagerank|run-frontend]" ; exit 1 ;;
esac
