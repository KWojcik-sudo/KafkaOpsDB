# KafkaOpsDB – architektura

## 1. Problem i cele

Chcemy móc szybko odpowiadać na pytania typu „gdzie jest wiadomość z `correlation-id = X`?”: w jakim topicu,
partycji i offsecie, z jakim timestampem i headerami. Pytanie może dotyczyć jednego topicu albo całego klastra.
Kafka sama nie ma indeksów: każde takie zapytanie to dziś liniowy skan topiców.

**Cele**

1. Indeksować wszystko poza `value`: topic, partition, offset, timestamp, timestamp type, key, rozmiary key/value,
   schema id z Confluent wire format, headery.
2. Format szyty pod strukturę wiadomości Kafki. Offsety i partycje mają kosztować ~0 B/wiersz, a headery
   (tablica par klucz–wartość) mają się dobrze kompresować i indeksować.
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
                  │ (master)│   │          │   │ per topic │   │  (KSEG)  │   │ Committer  │      │
                  └────┬────┘   └──────────┘   └───────────┘   └──────────┘   └─────┬──────┘      │
                       │ snapshot (offsety)                                           │            │
                       ▼                                                              ▼            │
                  ┌───────────────────────── Table (commit log + snapshot) ─────────────────┐     │
                  └───────▲──────────────────────────▲─────────────────────────▲────────────┘     │
                          │                          │                         │                  │
                  ┌───────┴──────┐           ┌───────┴──────┐          ┌───────┴───────┐          │
                  │  Kompaktor   │           │    Vacuum    │          │ Query engine  │◀── HTTP / CLI
                  │ (L0→L1, prom)│           │ (GC plików)  │          │ DataFusion +  │          │
                  └──────────────┘           └──────────────┘          │ cache (dysk)  │          │
                        │                                              └───────────────┘          │
                        └─────────────────────────────────────────────────────────────────────────┘
                                                     │
                                         S3 / ABFS / GCS / NFS (object_store)
```

| Crate | Odpowiedzialność |
|---|---|
| `kseg` | Format segmentu: enkodery, headery (shape + pivot), bloom filtry, `SegmentWriter`, `SegmentReader` (async range-ready) |
| `table` | Commit log, akcje, snapshot, checkpointy, optymistyczny commit z walidacją, vacuum |
| `ingest` | Planner, fetch z Kafki (rdkafka), bufor, polityka flusha, batch executor, kompaktor |
| `query` | Rewrite SQL, `TableProvider` dla DataFusion z pushdownem filtrów, UDF-y, lokalny cache |
| `server` | Binarka: konfiguracja, tryb `standalone`, HTTP API (axum), CLI, metryki Prometheus |

Główne zależności: `object_store`, `rdkafka`, `datafusion` / `arrow`, `sqlparser`, `zstd`, `xxhash-rust`,
`foyer`, `tokio`, `axum`, `serde`.

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
    topic=<topic>/date=<yyyy-mm-dd>/<segment-uuid>.kseg   # segmenty typu `topic`
    mixed/date=<yyyy-mm-dd>/<segment-uuid>.kseg           # segmenty typu `mixed`
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
| `key_size` | `INT` | `NULL` gdy key = null |
| `value_size` | `INT` | `NULL` gdy tombstone |
| `value_schema_id` | `INT` | z Confluent wire format (magic `0x00` + 4 B id), inaczej `NULL` |
| `headers` | `LIST<STRUCT<key STRING, value BINARY>>` | pełna lista headerów z zachowaniem kolejności i duplikatów |

`SELECT *` zwraca powyższe kolumny.

### Składnia headerów

```sql
SELECT * FROM topics WHERE headers.`correlation-id` = '632543-gdsf2-fgdsd2';
```

Parser (`sqlparser-rs`) przepisuje wyrażenie ``headers.`x` `` na wywołanie UDF `header('x')`, które zwraca
pierwszą wartość headera `x` jako string (`NULL`, gdy go nie ma). Dla duplikatów będzie `header_all('x')`
zwracające listę. `TableProvider` rozpoznaje `header('k') = 'v'` oraz `header('k') IN (...)` i używa ich
do pruningu (bloom filtry, słowniki).

### Wykonanie zapytania

1. Pinning snapshotu: najnowsza wersja logu.
2. Pruning segmentów na podstawie manifestu z commit logu: topic, partition, zakresy offsetów i timestampów.
   Manifest zawiera listę topiców także dla segmentów `mixed`.
3. Pobranie footerów (lokalny cache; segmenty są niemutowalne, więc cache nigdy nie wymaga invalidacji).
4. Wybór bloków: `topic_id`, `partition`, zone mapy.
5. Bloom filtry / słowniki per (blok, kolumna) dla predykatów równościowych.
6. Range-GET potrzebnych column chunków, skan, materializacja wierszy.
7. DataFusion robi resztę: projekcje, `ORDER BY`, `LIMIT`, agregacje.

**Koszt zapytań przekrojowych w v1.** Bez filtra po topicu sprawdzamy bloomy ze wszystkich segmentów
w oknie czasu. Przy skali TB/mies. i segmentach L1 ~512 MB to rząd 2–3 tys. segmentów na miesiąc. Indeksy
bloomowe leżą w lokalnym cache, więc to wystarcza, ale zalecane jest zawężenie po `timestamp`. Docelowym
rozwiązaniem jest globalny term-index (roadmapa).

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
- **Bufor:** kolumnowe buildery per topic w globalnym budżecie pamięci (domyślnie 1–4 GB).
- **Polityka flusha:**
  - bufor topicu ≥ `topic_segment_min_bytes` (domyślnie 16 MB) → segment typu `topic`;
  - po `max_flush_age` (domyślnie 30 s) pozostałe, mniejsze bufory → **jeden** segment `mixed`;
  - przekroczenie budżetu pamięci → flush największych buforów.

  Wszystkie segmenty z jednego flusha trafiają do jednego commitu razem z offsetami. Na środowisku testowym
  z 10 msg/min per topic powstaje więc 1 plik i 1 commit na `max_flush_age` dla całego klastra.
- **Pipeline z backpressure:** fetch → buffer → encode → upload (multipart) → commit. Batch N się
  uploaduje i commituje, podczas gdy batch N+1 już się czyta.
- **Recovery:** po starcie odtwarzany jest snapshot, z niego offsety, potem `seek()`. Zapisane,
  ale niezcommitowane pliki są orphanami do usunięcia przez vacuum.

## 7. Kompakcja i retencja

Kompaktor działa w tle w tym samym procesie. Każda operacja to jeden commit `remove_segment* + add_segment*`.

| Wejście | Wyjście | Reguła |
|---|---|---|
| L0 `topic` | L1 `topic` | per topic, per okno czasowe, docelowo ~512 MB |
| L0 `mixed` | L1 `mixed` | sklejanie po oknie czasowym (godzina → doba) |
| `mixed` (dowolny poziom) | `topic` + `mixed` | **promocja**: gdy topic w segmentach `mixed` z jednego okna przekroczy `topic_segment_min_bytes`, jego bloki są wyciągane do segmentu `topic` |

Blok zawsze należy do jednej pary (topic, partition), więc merge i promocja w większości kopiują
skompresowane column chunki bez dekodowania. Przemapować trzeba tylko identyfikatory w słownikach headerów
i kształtów, a chunki zależne od słowników (kolumna kształtów) są re-enkodowane.

Retencja per topic (`retention = "30d"`) to commit `remove_segment` dla segmentów, w których wszystkie wiersze
danego topicu są starsze niż próg. Segmenty `mixed` są przepisywane bez wygasłego topicu.

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
memory_budget = "2GiB"
topic_segment_min_bytes = "16MiB"
max_flush_age = "30s"

[index]
bloom_bits_per_value = 10
exclude_header_keys = []

[query]
listen = "0.0.0.0:8080"
cache_dir = "/var/cache/kafkaopsdb"
cache_size = "20GiB"
```

Metryki Prometheus pod `/metrics`: lag per partycja, opóźnienie i liczba konfliktów commitów, zajętość bufora,
liczba segmentów per poziom, hit-rate cache, latencja zapytań.

## 9. Roadmapa

**MVP (single node)**

| Etap | Zakres |
|---|---|
| M0 | Ta dokumentacja |
| M1 | `kseg`: enkodery, headery shape + pivot, bloom, writer / reader, typy `topic` i `mixed`; benchmark kompresji vs Parquet + zstd |
| M2 | `table`: commit log, put-if-absent, manifest, snapshot / checkpoint, walidacja konfliktów, vacuum |
| M3 | `ingest`: planner, fetch, bufor, polityka flusha, commit, recovery |
| M4 | `query`: tabela `topics`, rewrite SQL, `TableProvider` z pushdownem, cache, HTTP + CLI |
| M5 | Kompakcja L0 → L1, promocja `mixed` → `topic`, retencja |
| M6 | `standalone`, docker-compose (Kafka + MinIO), testy e2e |

**Po MVP**

- Distributed mode: master / worker / query ([distributed.md](distributed.md)).
- `fetchValue()`, potem dekodowanie przez Schema Registry.
- Globalny term-index: posortowane hashe wartości → (segment, blok), budowany przy kompakcji i shardowany
  po prefiksie hasha. Zamienia O(liczba segmentów) na O(1) range-GET na okno czasu.
- Opcjonalne zapytania po niezcommitowanym buforze indexera w trybie single-node (freshness ~sekundy).
