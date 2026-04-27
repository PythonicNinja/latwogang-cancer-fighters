# latwogang

Fetch all payments for the **Łatwogang x Cancer Fighters** fundraise on
[siepomaga.pl](https://www.siepomaga.pl/latwogang) and dump them to JSON + CSV.

## Requirements

- [`uv`](https://docs.astral.sh/uv/) (script uses inline `# /// script` header,
  so deps are resolved automatically — no manual install)
- Python 3.11+

## Usage

```sh
make download
```

Equivalent to:

```sh
./fetch_payments.py
```

Output: `payments.csv` — flattened, one row per payment, payer fields inlined.

## How it works

API: `GET https://www.siepomaga.pl/api/v1/payments`

Params:

- `target_type=Fundraise`
- `target_id=LZSw1Ox`
- `sort_by=biggest`
- `locale=pl`

Pagination is cursor-based. After each page, the last item's `id` and `amount`
are passed back as `after_id` and `after_value`. Loop stops when `data` is
empty.

### Idempotent + streaming

- On rerun, existing `payments.csv` is read, all known `id`s loaded into a set,
  and the script appends **only new rows** — safe to rerun any time.
- Each page is written + `flush()`ed immediately, so the CSV is always
  consistent mid-run and you can `tail -f` / crawl it while the fetch is still
  going.
- Delete `payments.csv` to force a full re-fetch from scratch.
