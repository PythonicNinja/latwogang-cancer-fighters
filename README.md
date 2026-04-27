# latwogang

Crawl public donations for the **Łatwogang x Cancer Fighters** fundraise on
[siepomaga.pl](https://www.siepomaga.pl/latwogang) and render a mobile-friendly
static dashboard ready for Cloudflare Pages.

## Requirements

- [`uv`](https://docs.astral.sh/uv/) — both Python scripts use the inline
  `# /// script` header, so deps resolve automatically (no manual install).
- Python 3.11+
- (optional) `wrangler` for `make deploy`.

## Targets

```sh
make download   # fetch payments → payments.csv (idempotent, append-only)
make web        # aggregate → web/data/stats.json + copy CSV
make serve      # local preview at http://localhost:8000
make deploy     # wrangler pages deploy web --project-name latwogang
make clean      # remove generated web/data/
```

Typical flow:

```sh
make download && make web && make serve
```

## Data fetch (`fetch_payments.py`)

API: `GET https://www.siepomaga.pl/api/v1/payments`

Params: `target_type=Fundraise`, `target_id=LZSw1Ox`, `sort_by=biggest`,
`locale=pl`.

Cursor pagination — after each page the last item's `id` and `amount` are
passed back as `after_id` + `after_value`. Loop stops on empty page.

**Idempotent + streaming**:
- Existing `payments.csv` is read, known `id`s loaded into a set, only new rows
  appended — safe to rerun.
- Each page is written + `flush()`ed immediately, so the CSV is consistent
  mid-run (`tail -f` works).
- Delete `payments.csv` to force a full re-fetch.

## Dashboard build (`build_web.py`)

Reads `payments.csv`, filters `state=confirmed`, computes:

- Totals (count, sum, mean, median, anonymous vs named).
- Top-20 donors by total (overall, companies, individuals).
- Top-20 single donations.
- Daily timeline + hourly distribution.
- Amount-bucket histogram.
- Longest comments.

Output:

- `web/data/stats.json` — precomputed answers (small, no client-side heavy
  lifting for first paint).
- `web/data/payments.csv.gz` — slimmed columns (`id, amount, at, name,
  company, comment`), gzipped. Lazy-loaded by the filter section and
  decompressed in-browser via `DecompressionStream`. Stays under Cloudflare
  Pages' 25 MiB-per-asset limit.

## Dashboard (`web/index.html`)

Mobile-first single-page app, no build step.

- **Tailwind v3** Play CDN — utility classes, dark mode default.
- **Alpine.js** — reactivity for tabs, filters, theme toggle.
- **Chart.js** — daily timeline, hourly distribution, amount histogram.
- **PapaParse** — streams `payments.csv` in a worker for the filter section.

Interactive bottom section: full-text search (name + comment), min/max PLN,
date range, only-with-comments, only-companies, sort, paginated table,
"download filtered CSV" button.

## Deploy to Cloudflare Pages

The `web/` directory is the deployable artifact (everything else is dev-only).

Drag-and-drop:

1. Build: `make web`.
2. Cloudflare Dashboard → Workers & Pages → Create → Pages → Direct upload.
3. Drop the `web/` folder.

CLI:

```sh
make deploy   # = npx wrangler pages deploy web --project-name latwogang
```

`web/_headers` sets long cache for `/assets/*` (immutable) and short cache for
`/data/*` (refreshed each rebuild).

Refresh data:

```sh
make download && make web && make deploy
```
