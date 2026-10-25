# Format segmentu KSEG (wersja 1)

KSEG to niemutowalny, kolumnowy plik z metadanymi wiadomości Kafki. Format zakłada strukturę danych,
którą znamy z góry, zamiast ogólnego schematu jak w Parquet:

- `topic` i `partition` są stałe w obrębie bloku, więc nie zajmują miejsca w wierszach;
- offsety rosną prawie zawsze o 1, więc zapisujemy tylko odchylenia;
- headery to zmienna tablica par klucz–wartość, ale w praktyce mało różnych kluczy i powtarzalny układ.
  Rozkładamy je na kolumnę „kształtu” i osobną kolumnę wartości per klucz.

## 1. Terminologia

| Pojęcie | Znaczenie |
|---|---|
| **segment** | jeden plik `.kseg`; 1 plik = 1 segment |
| **blok** | ≤ 65 536 wierszy **jednej pary (topic, partition)**, posortowanych rosnąco po offsecie |
| **column chunk** | dane jednej kolumny w jednym bloku; jednostka kompresji i odczytu (range-GET) |
| **typ segmentu** | `topic` (jeden topic, wiele partycji) lub `mixed` (wiele topiców) |
| **kształt (shape)** | uporządkowana lista `key_id` headerów jednej wiadomości |

Typy segmentów:

| Typ | Kod | Zawartość | Kiedy powstaje |
|---|---|---|---|
| `topic` | 0 | dokładnie jeden topic | flush bufora topicu ≥ `topic_segment_min_bytes`; kompakcja / promocja |
| `mixed` | 1 | ≥ 1 topic | flush po `max_flush_age` dla „chudych” buforów; kompakcja `mixed` |

Format bloku jest identyczny w obu typach. Różnią się tylko słownikiem topiców w footerze (w `topic`
ma on dokładnie 1 wpis) i polityką powstawania.

Bloki w pliku są posortowane po `(topic_id, partition, first_offset)`. Bloki tej samej pary
(topic, partition) mają rozłączne, rosnące zakresy offsetów.

## 2. Layout pliku

```
┌─────────────────────────────────────────────┐ offset 0
│ Header (8 B)                                │
├─────────────────────────────────────────────┤
│ Block 0: chunk c0 | chunk c1 | ...          │
│ Block 1: ...                                │
│ ...                                         │
├─────────────────────────────────────────────┤ index_section.offset
│ Index section (bloomy / zbiory wartości),   │
│ pogrupowane PO KOLUMNIE, potem po bloku     │
├─────────────────────────────────────────────┤ footer_offset
│ Footer (zstd)                               │
├─────────────────────────────────────────────┤
│ Trailer (16 B)                              │
└─────────────────────────────────────────────┘
```

**Header (8 B):** `"KSEG"` (4 B) | `format_version: u16 LE` (= 1) | `segment_type: u8` | `reserved: u8` (= 0).

**Trailer (16 B):** `footer_len: u32 LE` | `footer_crc32c: u32 LE` | `format_version: u16 LE` | `reserved: u16` |
`"KSEG"` (4 B).

`footer_offset` i `footer_len` są zapisywane w manifeście segmentu w commit logu, więc reader zna je
z góry i pobiera footer jednym range-GET. Bez manifestu (np. narzędzia diagnostyczne) wystarczą
2 range-GETy: trailer, potem footer.

Index section leży osobno, a nie w footerze, bo przy dużych segmentach bloomy zajmują megabajty.
Footer musi pozostać mały, bo jest pobierany dla każdego segmentu kandydującego. Index section jest
pogrupowany po kolumnie, więc zapytanie o ``headers.`correlation-id` `` pobiera **jednym** range-GET
bloomy tej kolumny dla wszystkich bloków segmentu.

## 3. Konwencje kodowania

- Liczby stałej szerokości: little-endian.
- `varint`: unsigned LEB128. `zvarint`: zigzag + LEB128 (dla liczb ze znakiem).
- `bytes`: `varint len` + dane. `string`: `bytes` w UTF-8.
- `[T]`: `varint count` + elementy.
- Bitpacking: LSB-first, wartości o szerokości `bit_width` bitów, bez wyrównania między wartościami,
  dopełnienie zerami do pełnego bajtu na końcu.
- Hash wartości do indeksów: `xxh64(value_bytes, seed = 0)`.
- Sumy kontrolne: CRC32C.

## 4. Footer

Footer jest serializowany wg poniższej gramatyki i kompresowany zstd.

```
Footer {
  segment_id:        [u8; 16]          // UUID v7 (sortowalny po czasie utworzenia)
  segment_type:      u8
  level:             u8                // 0 = L0 (flush), 1+ = po kompakcji
  created_at_ms:     zvarint
  writer:            string            // np. "kafkaopsdb/0.1.0 node-1"
  topics:            [string]          // topic_id = indeks
  header_keys:       [string]          // key_id = indeks (klucze headerów Kafki są stringami)
  shapes:            [Shape]           // shape_id = indeks; shape 0 = [] (brak headerów)
  blocks:            [BlockMeta]
  index_section:     { offset: varint, length: varint }
  column_indexes:    [ColumnIndexMeta]
}

Shape { key_ids: [varint] }            // kolejność i duplikaty zachowane

BlockMeta {
  topic_id:          varint
  partition:         zvarint
  row_count:         varint            // 1..=65536
  first_offset:      zvarint
  last_offset:       zvarint
  ts_min:            zvarint           // ms
  ts_max:            zvarint
  chunks:            [ChunkMeta]
}

ChunkMeta {
  column_id:         varint
  offset:            varint            // absolutny offset w pliku
  length:            varint            // bajty na dysku
  uncompressed_len:  varint
  encoding:          u8
  compression:       u8                // 0 = none, 1 = zstd
  value_count:       varint            // liczba wartości (z nullami)
  null_count:        varint
  crc32c:            u32
}

ColumnIndexMeta {
  column_id:         varint
  offset:            varint            // absolutny offset regionu tej kolumny w index section
  length:            varint
  entries:           [BlockIndexEntry] // jeden wpis na blok, w którym kolumna występuje
}

BlockIndexEntry {
  block_idx:         varint
  kind:              u8                // 0 = brak, 1 = value_set, 2 = bloom
  rel_offset:        varint            // względem ColumnIndexMeta.offset
  length:            varint
}
```

Brak chunka danej kolumny w bloku oznacza, że kolumna jest w tym bloku pusta (np. żadna wiadomość
nie ma headera `x`).

## 5. Kolumny

| `column_id` | Kolumna | Nullable | Typowe kodowanie |
|---|---|---|---|
| 0 | `offset` | nie | `OffsetGaps` |
| 1 | `timestamp` | nie | `DeltaOfDelta` |
| 2 | `timestamp_type` | nie | `Const` / `Bitmap` (0 = CreateTime, 1 = LogAppendTime) |
| 3 | `key` | tak | `Dict` / `Uuid16` / `DecimalText` / `BytesPlain` |
| 4 | `key_size` | tak | obecny **tylko** gdy któryś klucz w bloku został obcięty (`max_key_bytes`); w przeciwnym razie `key_size = len(key)` |
| 5 | `value_size` | tak | `BitPackedFor`; `NULL` = tombstone |
| 6 | `value_schema_id` | tak | `Const` / `Rle`; `NULL`, gdy value nie zaczyna się od Confluent magic byte `0x00` |
| 7 | `header_shape` | nie | `Const` / `Rle` / `BitPackedFor` nad `shape_id` |
| 8–15 | zarezerwowane | | |
| 16 + `key_id` | wartości headera `header_keys[key_id]` | tak (header value w Kafce może być null) | dowolne kodowanie bajtów |

Readery ignorują nieznane `column_id` (kompatybilność w przód). Nieznane `encoding` oznacza błąd.

### 5.1 Headery: shape + pivot

Przykład trzech wiadomości:

```
m0: [correlation-id=a1, source=svc-a, content-type=json]
m1: [correlation-id=b2, source=svc-a, content-type=json]
m2: [correlation-id=c3, retry=1, retry=2]           // duplikat klucza
```

Słownik kluczy (segment): `0=correlation-id, 1=source, 2=content-type, 3=retry`.
Słownik kształtów (segment): `0=[]`, `1=[0,1,2]`, `2=[0,3,3]`.

Kolumny w bloku:

```
header_shape  (col 7) : 1, 1, 2               → zwykle Rle/Const ≈ 0 B/wiersz
col 16 correlation-id : a1, b2, c3            → Uuid16 / BytesPlain + zstd
col 17 source         : svc-a, svc-a          → Const / Dict
col 18 content-type   : json, json            → Const
col 19 retry          : 1, 2                  → DecimalText
```

Każda kolumna wartości zawiera wartości danego klucza w kolejności (wiersz, pozycja w wierszu).
`value_count` takiego chunka to liczba wystąpień klucza w bloku, a nie liczba wierszy.

**Rekonstrukcja wiersza `r`:** `shape = shapes[header_shape[r]]`. Dla każdego `key_id` z kształtu po kolei
bierzemy następną wartość z kolumny `16 + key_id`. Pozycję startową wiersza `r` w kolumnie klucza `k`
wyznacza suma prefiksowa wystąpień `k` w kształtach wierszy `0..r`. Kolumna kształtów jest mała,
więc liczy się to przy dekodowaniu bloku.

**Mapowanie odwrotne (wynik skanu → wiersz):** pozycja `p` w kolumnie klucza `k` → wiersz `r`, przez te same
sumy prefiksowe (wyszukiwanie binarne).

Słowniki kluczy i kształtów są per segment. W `mixed` są wspólne dla wszystkich topiców, które zwykle
i tak dzielą konwencje (`correlation-id`, `traceparent`, `content-type`). Gdy liczba kształtów
przekroczy `max_shapes` (domyślnie 65 536), writer zamyka segment i zaczyna nowy.

## 6. Kodowania

Kod `encoding` w `ChunkMeta`. Payload opisany poniżej jest kompresowany wg `compression`.

**Nullable.** Gdy `null_count > 0`, payload zaczyna się od mapy obecności zakodowanej jako `Rle` nad bitami
(1 = wartość obecna), a dalej idą tylko wartości niebędące nullami.

| Kod | Nazwa | Dla | Payload |
|---|---|---|---|
| 0 | `Const` | int / bytes | jedna wartość (`zvarint` lub `bytes`) |
| 1 | `BitPackedFor` | int | strony po 1024 wartości: `min: zvarint`, `bit_width: u8`, bitpacked `(v - min)` |
| 2 | `DeltaOfDelta` | int (timestamp) | `first: zvarint`, `first_delta: zvarint`, potem `BitPackedFor` nad zigzag(Δ²) |
| 3 | `OffsetGaps` | offset | `[ (row_delta: varint, extra_gap: varint) ]`. `offset[i] = first_offset + i + Σ extra_gap` dla luk w wierszach ≤ i. Luki to markery transakcji, compacted topics, aborted records |
| 4 | `Rle` | int / bool | `[ (run_len: varint, value: zvarint) ]` |
| 5 | `Bitmap` | bool | bitpacked z `bit_width = 1` |
| 6 | `BytesPlain` | bytes | `lengths` jako `BitPackedFor`, potem skonkatenowane dane |
| 7 | `Dict` | bytes | `dict: [bytes]`, potem indeksy jako `Rle` lub `BitPackedFor` (wybiera writer, flaga u8) |
| 8 | `Uuid16` | bytes | `case: u8` (0 = lower, 1 = upper), potem 16 B na wartość. Dozwolone tylko, gdy **każda** wartość to kanoniczny 36-znakowy UUID w jednym rozmiarze liter (round-trip bajt w bajt) |
| 9 | `DecimalText` | bytes | `BitPackedFor` nad wartościami. Dozwolone tylko, gdy każda wartość to kanoniczna liczba dziesiętna ASCII (bez wiodących zer, opcjonalny `-`, mieści się w i64) |

**Wybór kodowania (writer):** dla każdego chunka writer próbuje kandydatów dopuszczalnych dla typu
i danych, kompresuje i wybiera najmniejszy wynik. Kolejność próbowania jest heurystyczna, np. `Const`
gdy wszystkie wartości równe, `Dict` gdy NDV ≤ 25% wartości. Kompresja zstd (domyślnie poziom 3) jest
pomijana, gdy `skompresowane / nieskompresowane > 0.9`.

Przyszłe kodowania (nowe kody, bez zmiany wersji formatu): `HexText` (np. trace-id w `traceparent`),
`Fsst` (krótkie stringi), `Pattern` (stały prefiks / szablon).

## 7. Index section

Indeksowane są kolumny `key` (3) i wszystkie kolumny wartości headerów (16+), poza kluczami z
`exclude_header_keys`. Dla każdej pary (kolumna, blok) writer wybiera:

| `kind` | Kiedy | Zawartość | Semantyka |
|---|---|---|---|
| `value_set` | NDV w bloku ≤ `value_set_max` (domyślnie 64) | posortowane `u64` hashe wartości | dokładne (do kolizji xxh64) |
| `bloom` | NDV > `value_set_max` | Split Block Bloom Filter | false positive ~1% przy 10 bit/wartość |
| brak | kolumna nieindeksowana | | blok zawsze kandydatem |

**Bloom:** Split Block Bloom Filter zgodny z algorytmem z Parquet: bloki po 256 bitów (8 × u32), 8 stałych
soli, hash `xxh64`. Rozmiar = `next_pow2(ceil(NDV × bloom_bits_per_value / 8))` bajtów, minimum 32 B.
Bloom jest wymiarowany po NDV w bloku, a nie po liczbie wierszy.

**Narzut:** ~1,25 B na unikalną wartość na indeksowaną kolumnę o wysokiej kardynalności przy 10 bit/wartość.
Kolumny o niskiej kardynalności (`content-type`, `source`) kosztują pomijalnie mało (`value_set`).

**Lookup `header('k') = 'v'`:**

1. `key_id` z `header_keys`. Gdy klucza nie ma w słowniku, segment odpada bez dalszych odczytów.
2. `h = xxh64(utf8('v'))`.
3. Region index section dla kolumny `16 + key_id` (jeden range-GET, cache) → bloki, dla których
   `value_set` zawiera `h` lub bloom zwraca „może”.
4. Dla kandydatów: range-GET chunka wartości, dekodowanie, porównanie bajt w bajt → pozycje → wiersze.
5. Doczytanie chunków kolumn wymaganych przez projekcję tylko z bloków, w których coś trafiło.

## 8. Zapis

1. Bufor per (topic, partition): wiersze w kolejności offsetów (tak przychodzą z Kafki).
2. Przy flushu writer sortuje pary (topic, partition), tnie je na bloki (≤ 65 536 wierszy albo
   ≤ `max_block_bytes` danych nieskompresowanych, domyślnie 8 MiB) i buduje słowniki kluczy i kształtów.
3. Bloki są enkodowane i zapisywane strumieniowo (multipart upload), więc writer nie trzyma w pamięci
   całego pliku.
4. Na końcu zapisuje index section, footer i trailer.
5. Writer zwraca metadane dla manifestu: `footer_offset`, `footer_len`, zakresy per topic / partycja, rozmiary.

## 9. Szacunek rozmiaru (do weryfikacji w benchmarku M1)

Typowa wiadomość: key = UUID, headery `correlation-id` (UUID), `traceparent`, `content-type`, `source`,
`event-type`.

| Kolumna | Szac. B / wiersz |
|---|---|
| offset, partition, topic, timestamp_type, shape, schema_id | < 0,5 |
| timestamp | 1–2 |
| value_size | 1–2 |
| key (Uuid16) | 16 |
| correlation-id (Uuid16) | 16 |
| traceparent (BytesPlain + zstd; z `HexText` ~25) | ~40 |
| content-type, source, event-type | < 0,5 |
| indeksy (3 kolumny wysokiej kardynalności) | ~4 |
| **razem** | **~80** (surowe metadane ~250–300 B) |

## 10. Wersjonowanie

- `format_version` w header i trailer. Reader odrzuca nieznaną wersję.
- Nowe kodowania i kolumny są dodawane bez zmiany wersji: nowe `column_id` są ignorowane przez stare readery.
  Commit log deklaruje `min_reader_version`, żeby stare readery nie czytały segmentów z nieznanymi kodowaniami
  (patrz [commit-protocol.md](commit-protocol.md)).
