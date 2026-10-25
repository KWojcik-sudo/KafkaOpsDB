# Protokół commitów

Commit log w `_log/` jest jedynym źródłem prawdy o tym:

- które segmenty należą do tabeli,
- do jakiego offsetu każda partycja Kafki jest zindeksowana,
- które taski zostały wykonane.

Wszystkie trzy informacje zmieniają się **w jednym atomowym zapisie**. Na tym opiera się exactly-once.

## 1. Wymagania wobec storage'u

Jedyna potrzebna operacja atomowa to **put-if-absent** (utworzenie obiektu tylko wtedy, gdy nie istnieje).
W kodzie: `object_store::PutMode::Create`.

| Backend | Mechanizm |
|---|---|
| AWS S3 | `If-None-Match: *` (conditional writes, od 2024) |
| Azure Blob / ADLS Gen2 (ABFS) | `If-None-Match: *` |
| GCS | `ifGenerationMatch=0` |
| Lokalny FS / NFS | zapis do pliku tymczasowego + `link()` (błąd, gdy cel istnieje) – atomowe także na NFSv3/v4 |
| S3-compatible (MinIO, Ceph RGW, …) | wymagana obsługa `If-None-Match` – zależy od wersji |

Przy starcie node wykonuje **self-test**. Tworzy `_log/_probe-<uuid>` przez put-if-absent dwa razy
i oczekuje, że druga próba się nie powiedzie. Jeśli storage nie odrzuca drugiego zapisu, node odmawia startu.
Bez put-if-absent exactly-once nie jest możliwe, a cicha degradacja byłaby gorsza niż błąd.

Zakładamy też silną spójność LIST po zapisie. S3, ABFS i GCS ją zapewniają.

## 2. Pliki logu

```
_log/
  00000000000000000000.json              # wersja 0: protocol + table
  00000000000000000001.json
  ...
  00000000000000000100.checkpoint.json.zst
  _last_checkpoint                       # {"version": 100} – hint, zapisywany zwykłym PUT
```

Nazwa pliku to numer wersji, 20 cyfr z zerami wiodącymi, więc sortowanie leksykograficzne = numeryczne.
Plik wersji zawiera newline-delimited JSON, jedna akcja na linię.

## 3. Akcje

```jsonc
// tylko w wersji 0
{"protocol": {"min_reader_version": 1, "min_writer_version": 1}}
{"table":    {"id": "0192f3...", "created_at": 1761379200000}}

// w każdym commicie, dokładnie raz, jako pierwsza linia
{"commit_info": {
  "txn_id": "0192f4...",          // UUID v7, unikalny per próba commitu (wykrywanie własnego commitu po timeoutcie)
  "writer": "node-1",
  "timestamp": 1761379230000,
  "operation": "INGEST",          // INGEST | COMPACT | RETENTION | RESET
  "read_version": 41,             // wersja, na której writer bazował
  "task_id": "0192f3..."          // opcjonalnie: task z distributed mode / single-node
}}

// dodanie segmentu (manifest – wystarcza do pruningu bez otwierania pliku)
{"add": {
  "segment_id": "0192f4...",
  "path": "data/mixed/date=2026-10-25/0192f4....kseg",
  "segment_type": "mixed",        // topic | mixed
  "level": 0,
  "size": 1048576,
  "rows": 12000,
  "footer_offset": 1040000,
  "footer_len": 8576,
  "format_version": 1,
  "created_at": 1761379230000,
  "topics": [
    {"topic": "orders.v1", "ts_min": 1761379000000, "ts_max": 1761379229000,
     "partitions": [{"partition": 0, "first_offset": 1000, "last_offset": 1999, "rows": 1000}]}
  ]
}}

// usunięcie segmentu (kompakcja, retencja)
{"remove": {"segment_id": "0192f0...", "deletion_timestamp": 1761379230000}}

// przesunięcie offsetu partycji: precondition zapisany w samej akcji
{"offset_range": {"topic": "orders.v1", "topic_uuid": "Xy3...", "partition": 0,
                  "start": 1000, "end": 2000}}

// świadomy przeskok (retencja Kafki wyprzedziła indexer, ręczny reset)
{"offset_reset": {"topic": "orders.v1", "topic_uuid": "Xy3...", "partition": 0,
                  "start": 2000, "end": 50000, "reason": "log_start_offset_advanced"}}
```

### Semantyka offsetów

- `committed(tp)` = `end` ostatniej akcji `offset_range` / `offset_reset` dla partycji `tp`.
  Oznacza **następny** offset do przeczytania.
- `offset_range` jest poprawny tylko wtedy, gdy `start == committed(tp)`. Dla partycji, której jeszcze nie ma
  w tabeli, wymagany jest brak wpisu, a `start` to pierwszy przeczytany offset, zwykle log start offset
  albo zgodnie z polityką `initial_position = earliest | latest`.
- Wiersze z segmentów dodanych w tym samym commicie, dla partycji `tp`, mają offsety w `[start, end)`.
  Writer weryfikuje to przed commitem.
- `topic_uuid` to identyfikator topicu w Kafce. Gdy topic zostanie usunięty i utworzony ponownie pod tą samą
  nazwą, UUID się zmienia i powstaje nowy, niezależny strumień offsetów. Stare segmenty zostają do retencji.
- `end` może być większy niż ostatni zindeksowany offset + 1, bo w zakresie mogą być luki (markery
  transakcji, aborted records, compacted topics). Liczy się pozycja konsumenta po przeczytaniu zakresu.

## 4. Stan tabeli (snapshot)

Snapshot w wersji `v` powstaje przez złożenie akcji z wersji `0..=v`:

- `live_segments`: zbiór `add` bez odpowiadającego `remove`;
- `tombstones`: `remove` (do vacuum);
- `offsets`: `committed(tp)` per `(topic_uuid, partition)`;
- `recent_txns` i `recent_tasks`: `txn_id` i `task_id` z ostatnich N wersji (do wykrywania duplikatów).

Odczyt najnowszego stanu:

1. Odczytaj `_last_checkpoint`. Jeśli go nie ma lub jest uszkodzony, wybierz najnowszy checkpoint z LIST.
2. Wczytaj checkpoint wersji `c`.
3. Wykonaj LIST `_log/` z `start-after = <c>.json` i zastosuj kolejne wersje `c+1, c+2, …`. Wersje muszą być
   ciągłe: dziura oznacza uszkodzenie logu.

## 5. Algorytm commitu

```
fn commit(snapshot, actions) -> Result<Version, Conflict>:
    txn_id = uuid_v7()
    loop (max 10 prób, backoff z jitterem):
        v = snapshot.version + 1
        match put_if_absent("_log/{v}.json", [commit_info(txn_id, read_version = snapshot.version), ..actions]):
            Ok                    => return Ok(v)
            Err(AlreadyExists)    =>
                new = read_versions(snapshot.version + 1 ..= latest)
                if new.any(|e| e.commit_info.txn_id == txn_id):
                    return Ok(that_version)        // nasz zapis przeszedł, choć klient dostał błąd
                snapshot = snapshot.apply(new)
                validate(snapshot, actions)?       // Conflict → wywołujący porzuca pracę
            Err(Transient)        =>               // timeout / 5xx: wynik nieznany
                // nie zakładamy porażki – następna iteracja rozstrzygnie przez txn_id
                continue
```

`validate` sprawdza względem aktualnego snapshotu:

| Akcja | Warunek |
|---|---|
| `offset_range` | `committed(tp) == start` (brak wpisu, gdy partycja nowa) |
| `offset_reset` | `committed(tp) == start` |
| `remove` | segment nadal w `live_segments` |
| `add` | brak warunku (UUID są unikalne) |
| `commit_info.task_id` | task nie występuje w `recent_tasks` |

Konflikt nie wymaga żadnego sprzątania po stronie writera. Jego pliki stają się orphanami i usunie je vacuum.

## 6. Dlaczego to daje exactly-once

1. Jedyny sposób, żeby wiersze z partycji `tp` trafiły do tabeli, to commit z akcją `offset_range(tp)`.
2. `offset_range` przechodzi walidację tylko wtedy, gdy zaczyna się dokładnie tam, gdzie skończył poprzedni.
   Zcommitowane zakresy partycji tworzą więc ciąg przyległych, rozłącznych przedziałów, bez dziur i nakładania.
3. Dane i offsety są w jednym obiekcie tworzonym atomowo. Nie istnieje stan „dane są, offset nie”
   ani „offset jest, danych nie ma”.
4. Kompakcja i retencja (`remove` + `add`) nie dotykają offsetów. Kompakcja przepisuje dokładnie te same wiersze.

Wniosek: dla każdej partycji zbiór wierszy w żywych segmentach to dokładnie rekordy z zakresów
`offset_range`, pomniejszone o to, co usunęła retencja. Nie zależy to od liczby writerów, crashy, timeoutów
ani poprawności lease'ów w distributed mode.

## 7. Scenariusze awarii

| Scenariusz | Skutek |
|---|---|
| Crash przed końcem uploadu | Brak commitu, niedokończony multipart (sprzątany lifecycle rule / vacuum), po restarcie ponowny odczyt z Kafki |
| Crash po uploadzie, przed commitem | Orphan, ponowny odczyt |
| Timeout PUT commitu, który faktycznie przeszedł | Następna próba dostaje `AlreadyExists`, znajduje swój `txn_id`, zwraca sukces |
| Dwóch writerów na tych samych partycjach | Jeden wygrywa, drugi dostaje konflikt offsetów i porzuca pracę |
| Zombie worker po wygaśnięciu lease'a | Jak wyżej |
| Kompakcja i retencja usuwają ten sam segment | Druga dostaje konflikt na `remove` i planuje ponownie |
| Topic usunięty i utworzony ponownie | Nowy `topic_uuid`, nowy strumień offsetów |
| Retencja Kafki wyprzedziła indexer | Commit `offset_reset` z jawnym `reason`, metryka `index_gap_records_total`, log ostrzegawczy |

## 8. Checkpointy

- Writer, który zcommitował wersję `v` z `v % checkpoint_interval == 0` (domyślnie 100), zapisuje
  `v.checkpoint.json.zst` przez put-if-absent. Zawiera on pełny snapshot: `protocol`, `table`, `live_segments`,
  `tombstones` nowsze niż `vacuum_grace`, `offsets`, `recent_tasks`. Potem nadpisuje `_last_checkpoint`.
- Checkpoint jest optymalizacją. Jego brak lub błąd zapisu nie wpływa na poprawność, bo log da się odtworzyć
  od poprzedniego checkpointu.
- Rozmiar: ~500 B na segment, czyli przy ~3 tys. segmentów miesięcznie i rocznej retencji ~20 MB po kompresji.
  Później można dodać checkpointy wieloczęściowe.

## 9. Vacuum i sprzątanie logu

**Segmenty usunięte (`remove`):** plik jest kasowany, gdy `deletion_timestamp < now - vacuum_grace`
(domyślnie 1 h). Zapytania pinują snapshot. Zapytanie trwające dłużej niż `vacuum_grace` może dostać
`NotFound`, wtedy query engine odświeża snapshot i ponawia plan raz.

**Orphany:** LIST `data/`, a w nim pliki, których nie ma ani w `live_segments`, ani w `tombstones`, i które
są starsze niż `orphan_grace` (domyślnie 24 h, musi przekraczać maksymalny czas uploadu + commitu).
Te pliki są kasowane.

**Log:** wpisy `_log/*.json` i checkpointy starsze niż najnowszy checkpoint oraz starsze niż `log_retention`
(domyślnie 7 dni) są kasowane. Najnowszego checkpointu nie kasujemy nigdy.

## 10. Kompatybilność

`protocol.min_reader_version` i `min_writer_version` są podnoszone przy zmianach niekompatybilnych,
np. nowym kodowaniu, którego stary reader nie zrozumie. Node o niższej wersji odmawia odczytu lub zapisu.
Zmiana protokołu to zwykły commit z akcją `protocol`.
