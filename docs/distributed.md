# Distributed mode: master i workery

Distributed mode używa tego samego formatu i protokołu commitów co single-node. Różni się tylko tym,
kto planuje pracę, a kto ją wykonuje.

Zasada nadrzędna: **poprawność danych zależy wyłącznie od protokołu commitów**
([commit-protocol.md](commit-protocol.md)). Lease'y, pending tasks i sam master służą efektywności,
czyli unikaniu zdublowanej pracy. Ich błędy mogą kosztować CPU i I/O, ale nigdy duplikaty ani dziury w danych.

## 1. Role

| Rola | Odpowiedzialność | Stan |
|---|---|---|
| **master** | planner (ingest + kompakcja + retencja), kolejka tasków, lease'y, vacuum | `tasks/pending/` na storage'u, lease'y w pamięci |
| **worker** | wykonuje taski: czyta Kafkę, enkoduje segmenty, uploaduje i **sam commituje** | bezstanowy |
| **query** | SQL nad snapshotem, lokalny cache | bezstanowy, cache na dysku |

Workery commitują same, z optymistyczną kontrolą konfliktów, więc master nie jest wąskim gardłem
zapisu i nie pośredniczy w danych.

**Single-node = master + worker + query w jednym procesie.** Te same struktury `Master` i `Worker` są połączone
kanałami `tokio::mpsc` zamiast HTTP. Pending tasks nie są tu potrzebne (`persist_pending = false`),
bo po restarcie planner odtwarza wszystko ze snapshotu.

## 2. Task

```jsonc
// tasks/pending/<task_id>.json
{
  "task_id": "0192f3...",                 // UUID v7
  "kind": "ingest",
  "created_at": 1761379200000,
  "base_version": 1234,                   // wersja logu, na podstawie której zaplanowano task
  "ranges": [
    {"topic": "orders.v1", "topic_uuid": "Xy3...", "partition": 0, "start": 1000, "end": 5000},
    {"topic": "orders.v1", "topic_uuid": "Xy3...", "partition": 1, "start": 980,  "end": 4100}
  ]
}

{
  "task_id": "0192f5...",
  "kind": "compact",
  "created_at": 1761379300000,
  "base_version": 1240,
  "inputs": ["0192e1...", "0192e2...", "0192e9..."],
  "output": {"segment_type": "topic", "level": 1, "promote_topics": ["orders.v1"]}
}
```

- Task `ingest` może obejmować wiele topiców. Gorące topici dostają osobne taski, a chude są grupowane.
  Worker stosuje politykę flusha `topic` / `mixed` w obrębie tasku i commituje wszystkie segmenty tasku
  w jednym commicie z `commit_info.task_id`.
- `end` jest ustalany przy planowaniu, więc task jest deterministyczny: ponowne wykonanie daje te same wiersze.
- **Maksymalnie jeden task in-flight na partycję.** Commit tasku k+1 dla partycji wymaga, żeby task k już
  był zcommitowany (warunek `start == committed`). Równoległość wynika z liczby partycji i topiców.
- Planner nie tworzy nowego tasku dla partycji, dopóki istnieje dla niej pending task. Nowe taski zaczynają się
  od `max(committed(tp), end ostatniego pending tasku)`.

## 3. API master ↔ worker (HTTP/JSON)

| Endpoint | Wejście | Wyjście |
|---|---|---|
| `POST /v1/tasks/acquire` | `{worker_id, kinds, capacity}` | `{task, lease_id, lease_expires_at}` albo `204` |
| `POST /v1/tasks/{id}/heartbeat` | `{lease_id}` | `{lease_expires_at}` albo `409 lease_lost` |
| `POST /v1/tasks/{id}/complete` | `{lease_id, outcome: committed{version} \| conflict}` | `200` |
| `POST /v1/tasks/{id}/fail` | `{lease_id, error, retryable}` | `200` |

Worker wysyła heartbeat co `lease_ttl / 3` (domyślnie TTL 60 s). Przy `409 lease_lost` przerywa pracę przed
commitem. To tylko optymalizacja: zombie, który mimo to zcommituje, i tak przejdzie walidację tylko wtedy,
gdy nikt inny nie zcommitował tego zakresu.

## 4. Cykl życia tasku w masterze

```
        create (zapis tasks/pending/<id>.json)
              │
              ▼
          PENDING ──acquire (po sprawdzeniu logu)──▶ LEASED
              ▲                                     │   │
              │      lease wygasł / fail(retryable) │   │ complete / commit z task_id widoczny w logu
              └─────────────────────────────────────┘   ▼
                                                      DONE (usunięcie pending)
          PENDING/LEASED ──conflict / obsolete──▶ DROPPED (usunięcie pending, replanning)
```

**Przed każdym przydzieleniem** master odświeża snapshot i sprawdza:

1. `task_id` jest w `recent_tasks` → **DONE**, usuń pending, nie przydzielaj.
2. Dla wszystkich `ranges` `committed(tp) >= end` → **DONE** (np. zcommitował go worker sprzed restartu mastera).
3. Dla któregoś zakresu `start != committed(tp)` → **DROPPED** (task nieaktualny), replanning partycji.
4. W przeciwnym razie → przydziel lease.

Zapis `tasks/pending/<id>.json` następuje **przed** udostępnieniem tasku, a usunięcie dopiero **po** zobaczeniu
commitu w logu. Na storage'u nie ma więc stanu „task wykonany, ale nikt o nim nie wie”: log zawsze
rozstrzyga, a pending jest tylko kolejką.

## 5. Restart mastera

1. Wczytaj snapshot (checkpoint + log).
2. LIST `tasks/pending/` i dla każdego tasku wykonaj kontrolę z sekcji 4: część jest DONE, część DROPPED,
   reszta wraca jako PENDING.
3. Lease'y nie są trwałe, więc po restarcie wszystkie są traktowane jak wygasłe. Worker, który nadal wykonuje
   task sprzed restartu, może go normalnie zcommitować. Master zobaczy commit i oznaczy task jako DONE.
   Najgorszy przypadek to ponowne przydzielenie tasku drugiemu workerowi i zdublowana praca, z której
   tylko jeden commit przejdzie.
4. Planner wznawia pracę od `max(committed, end pending tasków)`.

**Po co pending tasks, skoro wszystko da się odtworzyć ze snapshotu?** Dzięki nim taski są stabilne przez
restart (te same `task_id` i zakresy). Worker w trakcie pracy może dokończyć task, a master nie zaplanuje
w tym czasie nakładającego się tasku z innymi granicami. Taki task skończyłby się konfliktem i zmarnowaną
pracą jednej ze stron.

## 6. Wysoka dostępność mastera

Jeden aktywny master wystarcza, bo nie jest na ścieżce danych. Wybór aktywnego mastera to lock na storage'u:

- `_coordination/master.json` tworzony przez put-if-absent, a odnawiany przez conditional PUT `If-Match: <etag>`
  co `master_lease_ttl / 3`;
- standby przejmuje lock po wygaśnięciu `expires_at` (conditional PUT na ostatni widziany etag).

Dwóch masterów naraz, np. przy rozjechanych zegarach, oznacza w najgorszym razie zdublowane taski.
Poprawność danych pozostaje nienaruszona.

## 7. Query nodes w distributed mode

- Bezstanowe, każdy z własnym cache na dysku.
- Śledzą log przez LIST `_log/` `start-after <bieżąca wersja>` co 1–5 s. Opcjonalnie master może je powiadamiać.
- Później: rozproszone wykonanie zapytań (koordynator + przypisanie segmentów do node'ów po hashu `segment_id`
  dla lepszego hit-rate cache).
