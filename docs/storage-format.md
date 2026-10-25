# Format danych: Parquet / Avro na poziomach L0–L2

Dane leżą w standardowych formatach (Parquet, Avro), więc pliki da się czytać także DuckDB, Spark, Trino
czy `parquet-tools`. Własny, szyty pod Kafkę format kolumnowy jest odłożony do V2: jego pierwsza wersja
jest w historii gita (commit `675a09d`, crate `kseg`).

## 1. Schemat

Każdy plik ma ten sam schemat bazowy (Arrow). Odpowiada on tabeli SQL `topics`:

| Kolumna | Typ Arrow | Null | Uwagi |
|---|---|---|---|
| `topic` | `Utf8` | nie | |
| `partition` | `Int32` | nie | |
| `offset` | `Int64` | nie | |
| `timestamp` | `Timestamp(ms, "UTC")` | nie | |
| `timestamp_type` | `Utf8` | nie | `CreateTime` / `LogAppendTime` |
| `key` | `Binary` | tak | |
| `value_size` | `Int32` | tak | `NULL` = tombstone |
| `value_schema_id` | `Int32` | tak | z Confluent wire format (`0x00` + 4 B id) |
| `headers` | `List<Struct<key: Utf8, value: Binary?>>` | nie | pełna lista: kolejność, duplikaty i null value zachowane |

### Pivoty headerów (tylko Parquet)

Lista `headers` zachowuje wszystko, ale Parquet ani DataFusion nie potrafią pruningować po elementach listy.
Dlatego częste klucze headerów dostają w plikach Parquet osobne kolumny:

```
__h:<klucz>   Binary, nullable   – PIERWSZA wartość headera <klucz> w wierszu (NULL, gdy brak)
```

- Klucze wybierane per plik: `pivot.always` (zawsze, jeśli występują) + klucze obecne w ≥ `pivot.min_fraction`
  wierszy pliku (domyślnie 1%), maksymalnie `pivot.max_keys` (64).
- Każda kolumna pivot ma bloom filter (FPP 1%) i statystyki stron. Lookup po `correlation-id` pomija row groupy
  i strony, które na pewno nie zawierają wartości. Kolumny `headers` czytamy dopiero dla trafionych wierszy
  (late materialization).
- Lista kluczy pivot jest w manifeście (`pivot_keys`), a lista wszystkich kluczy występujących w pliku
  w `header_keys`. Plik bez danego klucza odpada już na etapie manifestu.
- Pivot przechowuje pierwszą wartość, więc ``headers.`x` = 'v'`` (pierwsza wartość) da się sprawdzić dokładnie.
  `header_all(headers, 'x')` (wszystkie wartości) sięga do listy.

## 2. Poziomy

| Poziom | Kto pisze | Format | Zawartość | Sortowanie | Rozmiar |
|---|---|---|---|---|---|
| **L0** | task ingestu (1 plik na task) | **Avro** gdy < `l0_parquet_min_rows` (25 000) wierszy, inaczej **Parquet Snappy** z pivotami | 1..N topiców (cały task) | (topic, partition, offset) | jak task |
| **L1** | task kompakcji | Parquet zstd(3) z pivotami | wiele topiców | (topic, partition, offset) | ~`l1_target_bytes` (256 MB) |
| **L2** | task kompakcji | Parquet zstd(3) z pivotami | **jeden topic** | (partition, offset) | ~`l2_target_bytes` (512 MB) |

Dlaczego Avro dla małych L0: zapis jest tani (wierszowy, brak metadanych kolumnowych), a pliki są małe,
więc czytamy je w całości. Pruning plików L0 odbywa się wyłącznie przez manifest (topic, partycja,
zakresy offsetów i czasu, `header_keys`).

### Przejścia między poziomami (taski mastera)

| Task | Wejście | Wyjście | Kiedy |
|---|---|---|---|
| `compact_l0` | najstarsze pliki L0 (do `compaction_max_input_bytes`) | pliki L1 | ≥ `l0_min_files` plików L0, najstarszy L0 ≥ `l0_max_age` albo suma L0 ≥ `l0_max_bytes` |
| `promote_l2` | pliki L1 zawierające topic T | pliki L2 tylko z T + pliki L1 bez T | szacowany rozmiar T w L1 ≥ `l2_promote_min_bytes` (128 MB) |
| `merge_l1` | małe pliki L1 (< `l1_target_bytes` / 2) | większe pliki L1 | ≥ 4 takie pliki (ciche topici zostają w L1 na stałe) |
| `merge_l2` | małe pliki L2 jednego topicu (< `l2_target_bytes` / 2) | większe pliki L2 | ≥ 4 takie pliki |
| `retention` | pliki z wierszami starszymi niż retencja topicu | brak / przepisane pliki | retencja per topic |

Każdy task kompakcji to jeden commit `remove* + add*` z `task_id`. Kompakcja nie dotyka offsetów Kafki,
więc gwarancja exactly-once z [commit-protocol.md](commit-protocol.md) się nie zmienia. Master nie planuje
dwóch tasków ze wspólnym plikiem wejściowym. Gdyby mimo to się zdarzyły (np. restart mastera), drugi
commit dostanie konflikt na `remove`.

Szacowany rozmiar topicu w pliku to `size × rows_topic / rows_file` (z manifestu).

## 3. Parametry Parquet

- Row group: 128 Ki wierszy. Statystyki na poziomie stron (page index), `sorting_columns` =
  (topic, partition, offset).
- Bloom filtry: `key` oraz wszystkie kolumny `__h:*`, FPP 1%, NDV = liczba wierszy row groupu.
- Kompresja: Snappy w L0 (szybki zapis na ścieżce ingestu), zstd(3) w L1/L2.
- Dictionary encoding: domyślny w parquet-rs, z automatycznym fallbackiem dla kolumn o wysokiej kardynalności.

## 4. Ścieżki

```
data/l0/date=<yyyy-mm-dd>/<uuid>.avro | .parquet
data/l1/date=<yyyy-mm-dd>/<uuid>.parquet
data/l2/topic=<topic>/date=<yyyy-mm-dd>/<uuid>.parquet
```

Data to dzień utworzenia pliku i służy tylko orientacji. O przynależności pliku do tabeli decyduje
wyłącznie commit log.
