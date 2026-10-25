# KafkaOpsDB – architektura

## 1. Problem i cele

Chcemy móc szybko odpowiadać na pytania typu „gdzie jest wiadomość z `correlation-id = X`?”: w jakim topicu,
partycji i offsecie, z jakim timestampem i headerami. Pytanie może dotyczyć jednego topicu albo całego klastra.
Kafka sama nie ma indeksów: każde takie zapytanie to dziś liniowy skan topiców.

**Cele**

1. Indeksować wszystko poza `value`: topic, partition, offset, timestamp, timestamp type, key, rozmiary key/value,
   schema id z Confluent wire format, headery.
2. Standardowe formaty (Parquet, Avro), w których headery (tablica par klucz–wartość) da się szybko
   przeszukiwać: częste klucze mają kolumny pivot z bloom filtrami ([storage-format.md](storage-format.md)).
3. Freshness rzędu dziesiątek sekund, a nie minut.
4. Exactly-once: brak dziur i duplikatów w indeksie także po crashach i restartach.
5. Deployment single-node: jedna aplikacja i podpięty storage (S3 / ABFS / GCS / NFS).
6. Możliwość skalowania (master + workery, osobne query node'y) bez zmiany formatu i protokołu.

**Poza zakresem (na teraz)**

- Przechowywanie `value`. W przyszłości `fetchValue()` będzie pobierać value bezpośrednio z Kafki,
  w granicach jej retencji.
- Pełny SQL ANSI (joiny między tabelami itp.). Dostajemy go częściowo za darmo z DataFusion, ale nie jest celem.
- Zastąpienie Kafki jako systemu przechowywania danych.

**Skala docelowa:** 10k–100k msg/s, TB metadanych miesięcznie. Single node powinien obsłużyć dolną połowę
tego zakresu.

## 2. Prior art

| Rozwiązanie | Co robi | Dlaczego nie wystarcza |
|---|---|---|
| Kafka Connect Iceberg Sink, Confluent Tableflow, Redpanda Iceberg Topics, AutoMQ Table Topic, WarpStream Tableflow | Kafka → tabela Iceberg | Latency minutowe; headery jako `list<struct>` słabo się kompresują i nie mają indeksu po wartości; potrzebny osobny silnik (Trino/Spark) i jobsy maintenance (`OPTIMIZE`, expire snapshots); kilka systemów do wdrożenia |
| Quickwit | Indeksowanie logów z Kafki do splitów na S3, checkpointy offsetów commitowane razem ze splitami | Architektonicznie najbliższy (pożyczamy ideę „dane + checkpoint w jednym commicie”), ale document-oriented: indeksuje payload JSON, nie optymalizuje struktury Kafki, wymaga doc-mappingu |
| Lenses, Conduktor, kcat, Kafka UI | Wyszukiwanie po topicach | Skan liniowy – O(n) per zapytanie, obciąża brokery |
| Debezium | CDC z baz danych **do** Kafki | Inny kierunek przepływu, nie dotyczy |

Wniosek: budujemy własny system. Protokół commitów zapożyczamy z Delta Lake / Iceberg (log wersji
z atomowym put-if-absent), a model „offsety commitowane razem z danymi” z Quickwita.

## 3. Komponenty

```
                        ┌──────────────────────── kafkaopsdb standalone ─────────────────────────┐
                        │                                                                         │
 Kafka ──fetch──▶ ┌─────────┐   ┌──────────┐   ┌───────────┐   ┌──────────┐   ┌────────────┐      │
 (assign+seek)    │ Planner │──▶│ Fetchers │──▶│  Buffer   │──▶│ Encoder  │──▶│ Uploader + │      │
                  │ (master)│   │          │   │  (task)   │   │ L0 Avro/ │   │ Committer  │      │
                  │         │   │          │   │           │   │ Parquet  │   │            │      │
                  └────┬────┘   └──────────┘   └───────────┘   └──────────┘   └─────┬──────┘      │
                       │ snapshot (offsety)                                           │            │
                       ▼                                                              ▼            │
                  ┌───────────────────────── Table (commit log + snapshot) ─────────────────┐     │
                  └───────▲──────────────────────────▲─────────────────────────▲────────────┘     │
                          │                          │                         │                  │
                  ┌───────┴──────┐           ┌───────┴──────┐          ┌───────┴───────┐          │
                  │  Kompaktor   │           │    Vacuum    │          │ Query engine  │◀── HTTP / CLI
                  │ L0→L1→L2     │           │ (GC plików)  │          │ DataFusion +  │          │
                  └──────────────┘           └──────────────┘          │ cache (dysk)  │          │
                        │                                              └───────────────┘          │
                        └─────────────────────────────────────────────────────────────────────────┘
                                                     │
                                         S3 / ABFS / GCS / NFS (object_store)
```

| Crate | Odpowiedzialność |
|---|---|
| `model` | Rekord, schemat Arrow, pivoty headerów, statystyki plików, zapis/odczyt Parquet i Avro |
| `table` | Commit log, akcje, snapshot, checkpointy, optymistyczny commit z walidacją, vacuum |
| `ingest` | Planner (master), fetch z Kafki (rdkafka), workery, taski kompakcji |
| `query` | Rewrite SQL, `TableProvider` dla DataFusion z pushdownem filtrów, UDF-y, lokalny cache |
| `server` | Binarka: konfiguracja, tryb `standalone`, HTTP API (axum), CLI, metryki Prometheus |

Główne zależności: `object_store`, `rdkafka`, `datafusion` / `arrow` / `parquet` / `arrow-avro`, `tokio`,
`axum`, `serde`.

## 4. Layout na storage'u

```
<root>/
  _log/
    00000000000000000000.json             # wersja 0: protocol + table metadata
    00000000000000000001.json             # commity (put-if-absent)
    ...
    00000000000000000100.checkpoint.json.zst  # pełny stan co N wersji
    _last_checkpoint                      # hint (best-effort, nieatomowy)
  data/
    l0/date=<yyyy-mm-dd>/<uuid>.avro|.parquet         # pliki z ingestu
    l1/date=<yyyy-mm-dd>/<uuid>.parquet               # kompakcja, wiele topiców
    l2/topic=<topic>/date=<yyyy-mm-dd>/<uuid>.parquet # kompakcja, jeden topic
  tasks/
    pending/<task-id>.json                # tylko distributed mode
  _coordination/
    master.json                           # tylko distributed mode: lock aktywnego mastera
```

Data w ścieżce służy tylko do porządku w buckecie i łatwiejszej ręcznej inspekcji. Źródłem prawdy
o tym, które pliki należą do tabeli, jest wyłącznie commit log. Szczegóły w
[commit-protocol.md](commit-protocol.md).

## 5. Model danych i SQL

### Tabela `topics`

Wszystkie wiadomości wszystkich topiców tworzą jedną logiczną tabelę `topics`. `topic` jest zwykłą kolumną,
więc zapytania przekrojowe (np. śledzenie `correlation-id` przez cały przepływ) są naturalne.

| Kolumna | Typ | Uwagi |
|---|---|---|
| `topic` | `STRING` | pushdown: `=`, `IN`, `LIKE 'prefix%'` |
| `partition` | `INT` | pushdown: `=`, `IN` |
| `offset` | `BIGINT` | pushdown: zakresy |
| `timestamp` | `TIMESTAMP(ms)` | pushdown: zakresy |
| `timestamp_type` | `STRING` | `CreateTime` / `LogAppendTime` |
| `key` | `BINARY` | pushdown: `=`; wyświetlany jako UTF-8 lub base64 |
| `value_size` | `INT` | `NULL` gdy tombstone |
| `value_schema_id` | `INT` | z Confluent wire format (magic `0x00` + 4 B id), inaczej `NULL` |
| `headers` | `LIST<STRUCT<key STRING, value BINARY>>` | pełna lista headerów z zachowaniem kolejności i duplikatów |

`SELECT *` zwraca powyższe kolumny.

### Składnia headerów

```sql
SELECT * FROM topics WHERE headers.`correlation-id` = '632543-gdsf2-fgdsd2';
```

Parser (`sqlparser-rs`) przepisuje wyrażenie ``headers.`x` `` na wywołanie UDF `header(headers, 'x')`, które
zwraca pierwszą wartość headera `x` jako string (`NULL`, gdy go nie ma). Wszystkie wartości zwraca
`header_all(headers, 'x')`. `TableProvider` rozpoznaje `header(headers, 'k') = 'v'` i `... IN (...)` i używa
ich do pruningu: manifest (`header_keys`), bloom filtry i statystyki kolumn pivot `__h:k`.

### Wykonanie zapytania

1. Pinning snapshotu: najnowsza wersja logu.
2. Pruning plików na podstawie manifestu z commit logu: topic, partition, zakresy offsetów i timestampów,
   `header_keys`. Dotyczy wszystkich poziomów, w tym plików L0 (Avro).
3. Parquet: metadane pliku (cache), pruning row groupów po statystykach i bloom filtrach (`key`, `__h:*`),
   `RowFilter` na kolumnie pivot, a ciężka kolumna `headers` czytana tylko dla trafionych wierszy.
4. Avro (małe pliki L0): odczyt w całości z cache i filtr w pamięci.
5. DataFusion robi resztę: projekcje, `ORDER BY`, `LIMIT`, agregacje.

Pliki są niemutowalne, więc lokalny cache (pamięć + dysk) nigdy nie wymaga invalidacji, tylko eviction.

**Koszt zapytań przekrojowych.** Bez filtra po topicu sprawdzamy bloom filtry wszystkich plików w oknie czasu,
w których występuje dany klucz headera. Metadane Parquet leżą w cache, ale zalecane jest zawężenie po
`timestamp`. Docelowym rozwiązaniem jest globalny term-index (roadmapa).

### Późniejsze rozszerzenia SQL

- `fetchValue(topic, partition, offset)`: batched async UDF, który pobiera value z Kafki (assign + seek,
  grupowanie po partycjach).
- `decodeValue(...)`: dekodowanie przez Schema Registry (Avro / Protobuf / JSON Schema) na podstawie
  `value_schema_id`.

## 6. Indexer

Szczegóły protokołu i tasków są w [commit-protocol.md](commit-protocol.md) i [distributed.md](distributed.md).
Tu jest skrót.

- **Klient Kafki:** `rdkafka` z `assign()` + `seek()`, `enable.auto.commit=false`,
  `isolation.level=read_committed`. Nie używamy consumer group: offsety żyją wyłącznie w commit logu.
- **Planner (microbatch):** co tick odświeża metadane (lista topiców, high watermarki; cache ~30 s) i liczy lag
  per partycja względem offsetów ze snapshotu. Score topicu = f(lag w bajtach, wiek najstarszej
  niezindeksowanej wiadomości, waga / SLA topicu). Planner wybiera możliwie mało topiców, ale każdy w całości
  (wszystkie partycje z lagiem), tak żeby wypełnić budżet pamięci batcha. Dzięki temu dane z jednego topicu
  lądują obok siebie.
- **Task:** deterministyczny zakres `(topic, [{partition, start_offset, end_offset}])`. `end_offset` jest
  ustalany w chwili planowania: `min(start + max_records, high_watermark)`.
- **Bufor:** task trzyma swoje rekordy w pamięci, a rozmiar tasku ogranicza `task_max_bytes`. Budżet pamięci
  to `task_max_bytes × parallelism`.
- **Kiedy topic jest gotowy do zapisu:**
  - szacowany rozmiar lagu ≥ `flush_min_bytes` (domyślnie 16 MB) → topic dostaje własne taski,
    a więc i własne pliki L0;
  - lag czeka ≥ `max_flush_age` (domyślnie 30 s) → ciche topici są pakowane razem w jeden task.
- **Zapis:** task = jeden plik L0 (Avro poniżej 25 000 wierszy, inaczej Parquet Snappy z pivotami),
  posortowany po (topic, partition, offset), i jeden commit razem z offsetami. Na środowisku testowym
  z 10 msg/min per topic powstaje 1 plik i 1 commit na `max_flush_age` dla całego klastra.
- **Równoległość:** taski z jednej rundy planowania wykonuje równolegle pula workerów (`parallelism`).
- **Recovery:** po starcie odtwarzany jest snapshot, z niego offsety, potem `seek()`. Zapisane,
  ale niezcommitowane pliki są orphanami do usunięcia przez vacuum.

## 7. Kompakcja i retencja

Master (planner) tworzy taski kompakcji, a wykonują je te same workery co ingest. Poziomy, reguły i formaty
opisuje [storage-format.md](storage-format.md):

- L0 (Avro / Parquet Snappy) → **L1**: Parquet zstd, wiele topiców, posortowany po (topic, partition, offset);
- L1 → **L2**: gdy topic uzbiera w L1 ≥ 128 MB, dostaje pliki L2 tylko dla siebie; ciche topici zostają w L1;
- merge małych plików w L1 i w L2 (per topic);
- retencja per topic (`retention = "30d"`): pliki całkowicie wygasłe są usuwane, a mieszane przepisywane
  bez wygasłych wierszy.

Każdy task to jeden commit `remove* + add*`, więc wynik kompakcji jest widoczny atomowo.

## 8. Deployment

```
kafkaopsdb standalone --config kafkaopsdb.toml   # MVP: wszystko w jednym procesie
kafkaopsdb sql "SELECT ..."                      # klient CLI do HTTP API
kafkaopsdb master | worker | query               # później: osobne role
```

Przykładowa konfiguracja:

```toml
[storage]
url = "s3://my-bucket/kafkaopsdb"     # az://container/prefix, gs://..., file:///mnt/nfs/kafkaopsdb

[kafka]
bootstrap_servers = "broker:9092"
include_topics = [".*"]
exclude_topics = ["^__.*", "^_schemas$"]
# security.protocol, sasl.* – przekazywane wprost do librdkafka

[ingest]
parallelism = 4
task_max_bytes = "256MiB"
flush_min_bytes = "16MiB"
max_flush_age = "30s"
l0_parquet_min_rows = 25000

[compaction]
l0_min_files = 8
l0_max_age = "5m"
l1_target_bytes = "256MiB"
l2_promote_min_bytes = "128MiB"
l2_target_bytes = "512MiB"

[pivot]
always = ["correlation-id"]
min_fraction = 0.01
max_keys = 64

[query]
listen = "0.0.0.0:8080"
cache_dir = "/var/cache/kafkaopsdb"
cache_size = "20GiB"
```

Metryki Prometheus pod `/metrics`: lag per partycja, opóźnienie i liczba konfliktów commitów, zajętość bufora,
liczba plików per poziom, hit-rate cache, latencja zapytań.

## 9. Roadmapa

**MVP (single node)**

| Etap | Zakres |
|---|---|
| M0 | Dokumentacja |
| M2 | `table`: commit log, put-if-absent, manifest, snapshot / checkpoint, walidacja konfliktów, vacuum |
| M3 | `ingest`: planner, fetch, workery, commit, recovery |
| R1–R2 | `model`: schemat, pivoty, Parquet/Avro; ingest pisze pliki L0 |
| R3 | Taski kompakcji L0 → L1 → L2, merge, retencja |
| R4 | `query`: tabela `topics`, rewrite SQL, `TableProvider` z pruningiem, cache |
| R5 | `standalone`, HTTP + CLI, docker-compose, testy e2e |

(M1, czyli własny format `kseg`, został odłożony do V2.)

**Po MVP**

- Distributed mode: master / worker / query ([distributed.md](distributed.md)).
- `fetchValue()`, potem dekodowanie przez Schema Registry.
- Globalny term-index: posortowane hashe wartości headerów → plik / row group, budowany przy kompakcji.
- V2: własny format kolumnowy szyty pod Kafkę (punkt wyjścia: crate `kseg` w historii gita).
- Opcjonalne zapytania po niezcommitowanym buforze indexera w trybie single-node (freshness ~sekundy).
