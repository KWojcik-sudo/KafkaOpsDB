# KafkaOpsDB

Indeks metadanych wiadomości Kafki (topic, partition, offset, timestamp, key, headers, rozmiary – wszystko poza `value`)
przechowywany jako Parquet / Avro na object storage (S3 / ABFS / GCS / NFS), z silnikiem SQL do
szybkich point-lookupów:

```sql
SELECT * FROM topics
WHERE topic = 'my.topic.v1' AND headers.`correlation-id` = '632543-gdsf2-fgdsd2';

-- przekrojowo po wszystkich topicach
SELECT topic, partition, offset, timestamp FROM topics
WHERE headers.`correlation-id` = '632543-gdsf2-fgdsd2';
```

Założenia:

- **Jedna binarka.** Tryb `standalone` = indexer + kompaktor + vacuum + query engine w jednym procesie.
  Do działania potrzebny jest tylko klaster Kafki i bucket / kontener / katalog NFS.
- **Exactly-once.** Offsety Kafki są commitowane atomowo razem z danymi w commit logu na storage'u
  (nie w consumer group).
- **Bez zewnętrznych jobów `OPTIMIZE`.** Kompakcja małych plików jest wbudowana.
- **Skalowanie opcjonalne.** Tryb master + workery używa tego samego kodu i tego samego protokołu commitów.

## Dokumentacja

| Dokument | Zawartość |
|---|---|
| [docs/architecture.md](docs/architecture.md) | Cele, prior art, komponenty, model danych, SQL, deployment, roadmapa |
| [docs/storage-format.md](docs/storage-format.md) | Schemat, pivoty headerów, poziomy L0 (Avro/Parquet) → L1 → L2 |
| [docs/commit-protocol.md](docs/commit-protocol.md) | Commit log, checkpointy, konflikty, exactly-once, vacuum |
| [docs/distributed.md](docs/distributed.md) | Master / worker, lease'y, pending tasks, recovery |

## Quickstart

```bash
cargo build --release -p kafkaopsdb
cp deploy/kafkaopsdb.toml kafkaopsdb.toml   # set [storage] url and [kafka] bootstrap_servers
target/release/kafkaopsdb standalone --config kafkaopsdb.toml

target/release/kafkaopsdb sql "SELECT topic, partition, offset, timestamp FROM topics WHERE headers.\`correlation-id\` = 'abc'"
target/release/kafkaopsdb sql --format json "SELECT topic, COUNT(*) FROM topics GROUP BY topic"
target/release/kafkaopsdb status
```

HTTP API: `POST /v1/query` (`{"sql": "...", "format": "json" | "ndjson" | "table"}`), `GET /v1/status`,
`GET /metrics` (Prometheus), `GET /health`.

The full stack (Kafka + MinIO + KafkaOpsDB) can be started with `docker compose -f deploy/docker-compose.yml up --build`.

## Crates

| Crate | Role |
|---|---|
| `crates/model` | record schema, header pivots, Parquet/Avro files |
| `crates/table` | commit log on object storage (exactly-once offsets, checkpoints, vacuum) |
| `crates/ingest` | Kafka source, planner, ingest workers, compaction tasks |
| `crates/query` | SQL on DataFusion with manifest / statistics / bloom filter pruning, cache |
| `crates/server` | `kafkaopsdb` binary: standalone mode, HTTP API, CLI |

## Tests

```bash
cargo test --workspace   # unit, property and in-process end-to-end tests
scripts/e2e.sh           # real Kafka (KRaft) + standalone, kill -9 / restart, SQL assertions
```
