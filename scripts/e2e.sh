#!/usr/bin/env bash
# End-to-end test: real Kafka (KRaft, single node, from the Apache tarball) + kafkaopsdb standalone
# on file:// storage. Produces data (including aborted transactions), kills the indexer with -9
# mid-ingest, restarts it and checks exactly-once indexing and SQL results.
#
#   scripts/e2e.sh            # KEEP=1 leaves Kafka and the server running afterwards
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=${E2E_DIR:-$ROOT/.e2e}
KAFKA_VERSION=${KAFKA_VERSION:-4.3.2}
KAFKA_PORT=${KAFKA_PORT:-39092}
HTTP=${HTTP:-127.0.0.1:18080}
KAFKA_DIR=$WORK/kafka_2.13-$KAFKA_VERSION
BIN=$ROOT/target/release/kafkaopsdb
PRODUCE=$ROOT/target/release/examples/produce
BROKERS=localhost:$KAFKA_PORT

log() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
fail() { printf '\033[31mFAIL: %s\033[0m\n' "$*"; exit 1; }

SERVER_PID=
cleanup() {
  if [[ "${KEEP:-0}" == 1 ]]; then return; fi
  [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null || true
  "$KAFKA_DIR/bin/kafka-server-stop.sh" >/dev/null 2>&1 || true
}
trap cleanup EXIT

mkdir -p "$WORK"
if [[ ! -x "$KAFKA_DIR/bin/kafka-server-start.sh" ]]; then
  log "downloading Kafka $KAFKA_VERSION"
  curl -fsSL "https://downloads.apache.org/kafka/$KAFKA_VERSION/kafka_2.13-$KAFKA_VERSION.tgz" | tar xz -C "$WORK"
fi

log "starting Kafka (KRaft) on $BROKERS"
"$KAFKA_DIR/bin/kafka-server-stop.sh" >/dev/null 2>&1 || true
rm -rf "$WORK/kafka-logs" "$WORK/table" "$WORK/cache"
cat > "$WORK/server.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:$((KAFKA_PORT + 1))
listeners=PLAINTEXT://localhost:$KAFKA_PORT,CONTROLLER://localhost:$((KAFKA_PORT + 1))
advertised.listeners=PLAINTEXT://localhost:$KAFKA_PORT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=$WORK/kafka-logs
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
EOF
"$KAFKA_DIR/bin/kafka-storage.sh" format -t "$("$KAFKA_DIR/bin/kafka-storage.sh" random-uuid)" -c "$WORK/server.properties" >/dev/null
KAFKA_HEAP_OPTS="-Xmx1G -Xms512M" LOG_DIR="$WORK/kafka-app-logs" "$KAFKA_DIR/bin/kafka-server-start.sh" -daemon "$WORK/server.properties"
for _ in $(seq 60); do
  "$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server "$BROKERS" --list >/dev/null 2>&1 && break
  sleep 1
done

topic() { "$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server "$BROKERS" --create --topic "$1" --partitions "$2" >/dev/null; }
topic orders 6
topic payments 3
topic audit 1
topic tx.orders 2
for i in $(seq -w 1 20); do topic "quiet-$i" 1; done

log "building release binaries"
(cd "$ROOT" && cargo build --release -q -p kafkaopsdb && cargo build --release -q -p ingest --example produce)

log "producing phase 1"
produce() { "$PRODUCE" brokers="$BROKERS" "$@"; }
N_ORDERS=200000
produce topic=orders count=$N_ORDERS corr_step=1 >/dev/null
produce topic=payments count=50000 corr_step=6 >/dev/null
produce topic=audit count=5000 corr_step=60 >/dev/null
TX_COMMITTED=$(produce topic=tx.orders count=20000 corr_step=7 transactional=true batch=500 abort_every=3)
for i in $(seq -w 1 20); do produce topic="quiet-$i" count=10 start=$((10#$i * 1000000)) >/dev/null; done
echo "tx.orders committed: $TX_COMMITTED of 20000"

cat > "$WORK/kafkaopsdb.toml" <<EOF
node_id = "e2e"
[storage]
url = "file://$WORK/table"
checkpoint_interval = 20
[kafka]
bootstrap_servers = "$BROKERS"
fetch_timeout = "10s"
[ingest]
parallelism = 4
task_max_bytes = "32MiB"
flush_min_bytes = "4MiB"
max_flush_age = "3s"
metadata_refresh = "1s"
tick = "500ms"
[compaction]
l0_min_files = 4
l0_max_age = "20s"
l2_promote_min_bytes = "2MiB"
merge_min_files = 3
tick = "2s"
[pivot]
always = ["correlation-id"]
[query]
listen = "$HTTP"
cache_dir = "$WORK/cache"
[vacuum]
interval = "15s"
tombstone_grace = "10s"
orphan_grace = "30s"
EOF

start_server() {
  "$BIN" standalone --config "$WORK/kafkaopsdb.toml" >>"$WORK/server.log" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 60); do curl -fs "http://$HTTP/health" >/dev/null 2>&1 && return; sleep 0.5; done
  fail "server did not start (see $WORK/server.log)"
}

wait_caught_up() {
  for _ in $(seq 300); do
    lag=$(curl -fs "http://$HTTP/v1/status" | python3 -c 'import json,sys; s=json.load(sys.stdin)["ingest"]; print(s["total_lag"] if s["partitions"] else -1)' || echo -1)
    # transaction markers keep the high watermark one ahead per partition
    if [[ "$lag" -ge 0 && "$lag" -le 2 ]]; then return; fi
    sleep 1
  done
  fail "ingest did not catch up (lag $lag)"
}

q() {
  curl -fs -X POST "http://$HTTP/v1/query" -H 'content-type: application/json' \
    -d "$(python3 -c 'import json,sys; print(json.dumps({"sql": sys.argv[1]}))' "$1")"
}

log "starting kafkaopsdb standalone"
: > "$WORK/server.log"
start_server
wait_caught_up

log "producing phase 2 and killing the indexer with -9 mid-ingest"
produce topic=orders count=100000 start=$N_ORDERS corr_step=1 >/dev/null &
PRODUCER=$!
sleep 3
kill -9 "$SERVER_PID"
wait "$SERVER_PID" 2>/dev/null || true
wait "$PRODUCER"
N_ORDERS=$((N_ORDERS + 100000))
start_server
wait_caught_up
sleep 25  # let compaction move data to L1/L2

log "checking results"
STATUS=$(curl -fs "http://$HTTP/v1/status")
export STATUS TX_COMMITTED N_ORDERS
python3 - "$HTTP" <<'PY'
import json, os, sys, time, urllib.request

http = sys.argv[1]
def q(sql):
    req = urllib.request.Request(f"http://{http}/v1/query", data=json.dumps({"sql": sql}).encode(),
                                 headers={"content-type": "application/json"})
    t = time.time()
    with urllib.request.urlopen(req) as r:
        body = json.load(r)
    return body["rows"], (time.time() - t) * 1000

def check(cond, msg):
    print(("ok   " if cond else "FAIL ") + msg)
    if not cond:
        sys.exit(1)

status = json.loads(os.environ["STATUS"])
n_orders, tx = int(os.environ["N_ORDERS"]), int(os.environ["TX_COMMITTED"])
print("files:", json.dumps(status["table"]["files"]))

rows, _ = q("SELECT topic, COUNT(*) AS n FROM topics GROUP BY topic")
counts = {r["topic"]: r["n"] for r in rows}
check(counts.get("orders") == n_orders, f"orders count {counts.get('orders')} == {n_orders}")
check(counts.get("payments") == 50000, f"payments count {counts.get('payments')}")
check(counts.get("audit") == 5000, f"audit count {counts.get('audit')}")
check(counts.get("tx.orders") == tx, f"tx.orders count {counts.get('tx.orders')} == committed {tx} (aborted records skipped)")
check(all(counts.get(f"quiet-{i:02d}") == 10 for i in range(1, 21)), "20 quiet topics with 10 records each")

hw = {(p["topic"], p["partition"]): p["high_watermark"] for p in status["ingest"]["partitions"]}
rows, _ = q("SELECT topic, partition, COUNT(*) AS n, MAX(offset) AS max_offset FROM topics WHERE topic <> 'tx.orders' GROUP BY topic, partition")
check(all(r["n"] == hw[(r["topic"], r["partition"])] and r["max_offset"] == r["n"] - 1 for r in rows),
      "every non-transactional partition: COUNT(*) == high watermark and offsets 0..HW-1")

rows, _ = q("SELECT COUNT(*) AS dups FROM (SELECT topic, partition, offset FROM topics GROUP BY topic, partition, offset HAVING COUNT(*) > 1)")
check(rows[0]["dups"] == 0, "no duplicate (topic, partition, offset) after kill -9 and restart")

rows, ms = q("SELECT topic FROM topics WHERE headers.`correlation-id` = 'corr-00000060' ORDER BY topic")
check([r["topic"] for r in rows] == ["audit", "orders", "payments"], f"cross-topic lookup by correlation-id ({ms:.0f} ms)")
rows, ms = q("SELECT topic FROM topics WHERE topic = 'orders' AND headers.`correlation-id` = 'corr-00250000'")
check(len(rows) == 1, f"lookup of a record produced while the indexer was killed ({ms:.0f} ms)")
rows, _ = q("SELECT topic FROM topics WHERE headers.`correlation-id` = 'corr-00007000' ORDER BY topic")
check([r["topic"] for r in rows] == ["orders"], "messages of aborted transactions are not indexed")
rows, _ = q("SELECT topic FROM topics WHERE headers.`correlation-id` = 'corr-00003500' ORDER BY topic")
check([r["topic"] for r in rows] == ["orders", "tx.orders"], "committed transactional messages are indexed")
rows, _ = q("SELECT COUNT(*) AS n FROM topics WHERE topic = 'orders' AND value_schema_id = 17")
check(rows[0]["n"] == sum(1 for j in range(200000) if j % 3 == 0) + sum(1 for j in range(100000) if j % 3 == 0),
      "Confluent schema id extracted")
rows, _ = q("SELECT header_all(headers, 'retry') AS r FROM topics WHERE headers.`correlation-id` = 'corr-00000010' AND topic = 'orders'")
check(rows and rows[0]["r"] == ["1", "2"], "duplicate header keys preserved")
levels = {f["level"] for f in status["table"]["files"]}
check(levels & {1, 2}, f"compaction produced L1/L2 files (levels {sorted(levels)})")
PY

log "e2e passed"
