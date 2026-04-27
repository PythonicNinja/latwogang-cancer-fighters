#!/usr/bin/env -S uv run --no-config --quiet --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Aggregate payments.csv into web/data/stats.json + copy of CSV for the dashboard."""

from __future__ import annotations

import csv
import gzip
import json
import shutil
import statistics
import sys
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any

ROOT = Path(__file__).parent
CSV_FILE = ROOT / "payments.csv"
WEB_DIR = ROOT / "web"
DATA_DIR = WEB_DIR / "data"
STATS_FILE = DATA_DIR / "stats.json"
CSV_GZ = DATA_DIR / "payments.csv.gz"

SLIM_FIELDS = ["id", "amount", "at", "name", "company", "comment"]

GROSZE_PER_PLN = 100
TOP_N = 20
LONGEST_COMMENTS_N = 12

FUNDRAISE_ID = "LZSw1Ox"
FUNDRAISE_STATS_URL = (
    f"https://www.siepomaga.pl/api/v1/fundraises/{FUNDRAISE_ID}/stats?locale=pl"
)

POLISH_NAMES_M = {
    "adam", "adrian", "albert", "aleksander", "alfred", "andrzej", "antoni", "arkadiusz",
    "artur", "bartek", "bartłomiej", "bartosz", "błażej", "bogdan", "bogusław", "borys",
    "cezary", "czesław", "dariusz", "dawid", "denis", "dominik", "edward", "emil",
    "eryk", "filip", "franciszek", "gabriel", "grzegorz", "henryk", "hubert", "ignacy",
    "igor", "jacek", "jakub", "jan", "janusz", "jarosław", "jerzy", "józef",
    "julian", "kacper", "kajetan", "kamil", "karol", "kazimierz", "konrad", "konstanty",
    "kornel", "krystian", "krzysztof", "leon", "leonard", "lech", "leszek", "ludwik",
    "łukasz", "maciej", "marcel", "marcin", "marek", "marian", "mariusz", "mateusz",
    "michał", "mieczysław", "mikołaj", "miłosz", "mirosław", "nikodem", "norbert",
    "oliwier", "olaf", "oskar", "patryk", "paweł", "piotr", "przemysław", "radosław",
    "rafał", "robert", "roman", "ryszard", "sebastian", "stanisław", "stefan", "sylwester",
    "szymon", "tadeusz", "tobiasz", "tomasz", "tymon", "tymoteusz", "wacław", "wiktor",
    "wincenty", "witold", "władysław", "włodzimierz", "wojciech", "zbigniew", "zdzisław",
    "zenon", "zygmunt",
}

POLISH_NAMES_F = {
    "ada", "adela", "agata", "agnieszka", "aleksandra", "alicja", "alina", "amelia",
    "anastazja", "aneta", "anna", "antonina", "barbara", "beata", "blanka", "bogumiła",
    "bożena", "celina", "danuta", "dagmara", "diana", "dominika", "dorota", "edyta",
    "elżbieta", "emilia", "ewa", "ewelina", "felicja", "gabriela", "gabrysia", "genowefa",
    "grażyna", "halina", "hanna", "helena", "honorata", "iga", "ilona", "ines",
    "irena", "iwona", "izabela", "jadwiga", "jagoda", "janina", "joanna", "jolanta",
    "julia", "julita", "justyna", "kalina", "karolina", "katarzyna", "kinga", "klara",
    "klaudia", "kornelia", "krystyna", "ksenia", "laura", "lena", "lidia", "liliana",
    "lucyna", "łucja", "magdalena", "maja", "malwina", "małgorzata", "maria", "marianna",
    "marlena", "marta", "martyna", "marzena", "milena", "mira", "mirosława", "monika",
    "nadia", "natalia", "nikola", "nina", "ola", "olga", "oliwia", "patrycja",
    "paulina", "pola", "renata", "regina", "róża", "sabina", "sandra", "sara",
    "sonia", "stanisława", "stefania", "sylwia", "tamara", "teresa", "urszula", "wanda",
    "weronika", "wiktoria", "wiesława", "zofia", "zuzanna",
}

AMOUNT_BUCKETS = [
    ("<10 zł", 0, 10_00),
    ("10–50 zł", 10_00, 50_00),
    ("50–100 zł", 50_00, 100_00),
    ("100–500 zł", 100_00, 500_00),
    ("500 zł – 1 tys.", 500_00, 1_000_00),
    ("1–5 tys. zł", 1_000_00, 5_000_00),
    ("5–10 tys. zł", 5_000_00, 10_000_00),
    ("10–50 tys. zł", 10_000_00, 50_000_00),
    ("50–100 tys. zł", 50_000_00, 100_000_00),
    (">100 tys. zł", 100_000_00, float("inf")),
]


def parse_bool(v: str | None) -> bool:
    return (v or "").strip().lower() == "true"


def parse_row(r: dict[str, str]) -> dict[str, Any] | None:
    if r.get("state") != "confirmed":
        return None
    try:
        # API returns amount as PLN (with decimals); store as integer grosze internally.
        amount_grosze = round(float(r["amount"]) * 100)
    except (ValueError, KeyError, TypeError):
        return None
    return {
        "id": r.get("id") or "",
        "amount_grosze": amount_grosze,
        "currency": r.get("currency") or "PLN",
        "comment": (r.get("comment_text") or "").strip(),
        "at": r.get("state_changed_at") or "",
        "constant_help": parse_bool(r.get("constant_help")),
        "highlighted": parse_bool(r.get("highlighted")),
        "payments_count": int(r.get("payments_count") or 1),
        "payer_id": r.get("payer_id") or "",
        "payer_name": (r.get("payer_name") or "").strip(),
        "payer_company": parse_bool(r.get("payer_company")),
        "payer_url": r.get("payer_url") or "",
        "payer_avatar_url": r.get("payer_avatar_url") or "",
    }


def load_rows() -> list[dict[str, Any]]:
    with CSV_FILE.open(newline="", encoding="utf-8") as f:
        out = []
        for raw in csv.DictReader(f):
            row = parse_row(raw)
            if row is not None:
                out.append(row)
    return out


def compute_totals(rows: list[dict]) -> dict:
    amounts = [r["amount_grosze"] for r in rows]
    named_keys = {r["payer_id"] or r["payer_name"] for r in rows if r["payer_name"]}
    anon = sum(1 for r in rows if not r["payer_name"])
    return {
        "donations_count": len(rows),
        "unique_donors_named": len(named_keys - {""}),
        "anonymous_count": anon,
        "total_grosze": sum(amounts),
        "average_grosze": round(statistics.mean(amounts)) if amounts else 0,
        "median_grosze": round(statistics.median(amounts)) if amounts else 0,
        "biggest_grosze": max(amounts) if amounts else 0,
        "smallest_grosze": min(amounts) if amounts else 0,
        "constant_help_count": sum(1 for r in rows if r["constant_help"]),
    }


def donor_key(r: dict) -> str:
    return r["payer_id"] or r["payer_name"] or "__anon__"


def aggregate_by_donor(rows: list[dict]) -> list[dict]:
    by_key: dict[str, dict] = {}
    for r in rows:
        k = donor_key(r)
        if k == "__anon__":
            continue
        d = by_key.setdefault(
            k,
            {
                "key": k,
                "name": r["payer_name"] or "Anonim",
                "company": r["payer_company"],
                "url": r["payer_url"],
                "avatar_url": r["payer_avatar_url"],
                "total_grosze": 0,
                "count": 0,
            },
        )
        d["total_grosze"] += r["amount_grosze"]
        d["count"] += 1
        if not d["avatar_url"] and r["payer_avatar_url"]:
            d["avatar_url"] = r["payer_avatar_url"]
        if not d["url"] and r["payer_url"]:
            d["url"] = r["payer_url"]
    return sorted(by_key.values(), key=lambda x: x["total_grosze"], reverse=True)


def top_n(items: list[dict], n: int) -> list[dict]:
    return items[:n]


def to_single_row(r: dict) -> dict:
    return {
        "id": r["id"],
        "name": r["payer_name"] or "Anonim",
        "company": r["payer_company"],
        "amount_grosze": r["amount_grosze"],
        "comment": r["comment"],
        "at": r["at"],
        "url": r["payer_url"],
        "avatar_url": r["payer_avatar_url"],
    }


def top_singles(rows: list[dict], n: int) -> list[dict]:
    sorted_rows = sorted(rows, key=lambda r: r["amount_grosze"], reverse=True)
    return [to_single_row(r) for r in sorted_rows[:n]]


def by_day(rows: list[dict]) -> list[dict]:
    by: dict[str, dict] = defaultdict(lambda: {"count": 0, "total_grosze": 0})
    for r in rows:
        if len(r["at"]) < 10:
            continue
        date = r["at"][:10]
        by[date]["count"] += 1
        by[date]["total_grosze"] += r["amount_grosze"]
    return [{"date": d, **v} for d, v in sorted(by.items())]


def by_hour(rows: list[dict]) -> list[dict]:
    counts: Counter[int] = Counter()
    totals: dict[int, int] = defaultdict(int)
    for r in rows:
        at = r["at"]
        if len(at) < 13:
            continue
        try:
            h = int(at[11:13])
        except ValueError:
            continue
        counts[h] += 1
        totals[h] += r["amount_grosze"]
    return [
        {"hour": h, "count": counts.get(h, 0), "total_grosze": totals.get(h, 0)}
        for h in range(24)
    ]


def by_hour_per_day(rows: list[dict]) -> list[dict]:
    counts: dict[str, list[int]] = defaultdict(lambda: [0] * 24)
    totals: dict[str, list[int]] = defaultdict(lambda: [0] * 24)
    for r in rows:
        at = r["at"]
        if len(at) < 13:
            continue
        try:
            hour = int(at[11:13])
        except ValueError:
            continue
        date = at[:10]
        counts[date][hour] += 1
        totals[date][hour] += r["amount_grosze"]
    return [
        {"date": d, "counts": counts[d], "totals_grosze": totals[d]}
        for d in sorted(counts.keys())
    ]


def by_amount_bucket(rows: list[dict]) -> list[dict]:
    out = []
    for label, lo, hi in AMOUNT_BUCKETS:
        matched = [r["amount_grosze"] for r in rows if lo <= r["amount_grosze"] < hi]
        out.append(
            {"range": label, "count": len(matched), "total_grosze": sum(matched)}
        )
    return out


def longest_comments(rows: list[dict], n: int) -> list[dict]:
    with_c = [r for r in rows if r["comment"]]
    with_c.sort(key=lambda r: len(r["comment"]), reverse=True)
    return [
        {
            "name": r["payer_name"] or "Anonim",
            "comment": r["comment"],
            "amount_grosze": r["amount_grosze"],
            "at": r["at"],
        }
        for r in with_c[:n]
    ]


def comments_stats(rows: list[dict]) -> dict:
    return {
        "with_comment_count": sum(1 for r in rows if r["comment"]),
        "longest": longest_comments(rows, LONGEST_COMMENTS_N),
    }


def first_name_token(payer_name: str) -> str:
    parts = (payer_name or "").strip().split()
    return parts[0].lower() if parts else ""


def classify_gender(payer_name: str) -> str:
    n = first_name_token(payer_name)
    if not n:
        return "unknown"
    if n in POLISH_NAMES_M:
        return "M"
    if n in POLISH_NAMES_F:
        return "F"
    if len(n) > 2 and n.endswith(("a", "ia", "ka", "na")):
        return "F"
    return "unknown"


def by_first_name(rows: list[dict], n: int) -> list[dict]:
    agg: dict[str, dict] = {}
    for r in rows:
        if r["payer_company"]:
            continue
        first = first_name_token(r["payer_name"])
        if not first or len(first) < 2:
            continue
        d = agg.setdefault(
            first,
            {
                "name": first.capitalize(),
                "gender": classify_gender(r["payer_name"]),
                "count": 0,
                "total_grosze": 0,
            },
        )
        d["count"] += 1
        d["total_grosze"] += r["amount_grosze"]
    for d in agg.values():
        d["avg_grosze"] = round(d["total_grosze"] / d["count"]) if d["count"] else 0
    sorted_items = sorted(agg.values(), key=lambda x: x["count"], reverse=True)
    return sorted_items[:n]


def by_gender(rows: list[dict]) -> dict:
    out: dict[str, dict] = {
        "M": {"label": "Mężczyźni", "count": 0, "total_grosze": 0},
        "F": {"label": "Kobiety", "count": 0, "total_grosze": 0},
        "unknown": {"label": "Nieokreślone", "count": 0, "total_grosze": 0},
        "company": {"label": "Firmy", "count": 0, "total_grosze": 0},
    }
    for r in rows:
        if r["payer_company"]:
            key = "company"
        else:
            key = classify_gender(r["payer_name"])
        out[key]["count"] += 1
        out[key]["total_grosze"] += r["amount_grosze"]
    for d in out.values():
        d["avg_grosze"] = (
            round(d["total_grosze"] / d["count"]) if d["count"] else 0
        )
    return out


def fetch_fundraise_remote() -> dict | None:
    try:
        req = urllib.request.Request(
            FUNDRAISE_STATS_URL,
            headers={
                "Accept": "application/json",
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0 Safari/537.36"
                ),
            },
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read()).get("data")
    except (urllib.error.URLError, ValueError, KeyError) as e:
        print(f"fundraise stats fetch failed: {e}", file=sys.stderr)
        return None


def indexing_progress(rows_count: int, remote: dict | None) -> dict:
    total = (remote or {}).get("payments_count")
    if not total:
        return {
            "indexed": rows_count,
            "total": None,
            "pct": None,
            "complete": False,
            "remote_total_amount_pln": None,
        }
    pct = round(rows_count / total * 100, 2) if total else 0
    return {
        "indexed": rows_count,
        "total": total,
        "pct": pct,
        "complete": rows_count >= total,
        "remote_total_amount_pln": remote.get("amount"),
    }


def build_stats(rows: list[dict]) -> dict:
    donors = aggregate_by_donor(rows)
    companies = [d for d in donors if d["company"]]
    individuals = [d for d in donors if not d["company"]]
    biggest = max(rows, key=lambda r: r["amount_grosze"]) if rows else None
    remote = fetch_fundraise_remote()
    return {
        "indexing": indexing_progress(len(rows), remote),
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "currency": "PLN",
        "fundraise": {
            "title": "Łatwogang x Cancer Fighters",
            "slug": "latwogang",
            "url": "https://www.siepomaga.pl/latwogang",
        },
        "totals": compute_totals(rows),
        "biggest_single": to_single_row(biggest) if biggest else None,
        "top_donors_by_total": top_n(donors, TOP_N),
        "top_companies": top_n(companies, TOP_N),
        "top_individuals": top_n(individuals, TOP_N),
        "top_single_donations": top_singles(rows, TOP_N),
        "by_day": by_day(rows),
        "by_hour": by_hour(rows),
        "by_hour_per_day": by_hour_per_day(rows),
        "by_amount_bucket": by_amount_bucket(rows),
        "by_first_name": by_first_name(rows, TOP_N),
        "by_gender": by_gender(rows),
        "comments": comments_stats(rows),
    }


def slim_csv_text(rows: list[dict]) -> str:
    buf = StringIO()
    w = csv.DictWriter(buf, fieldnames=SLIM_FIELDS)
    w.writeheader()
    for r in rows:
        w.writerow(
            {
                "id": r["id"],
                "amount": f"{r['amount_grosze'] / GROSZE_PER_PLN:.2f}",
                "at": r["at"],
                "name": r["payer_name"],
                "company": "1" if r["payer_company"] else "0",
                "comment": r["comment"],
            }
        )
    return buf.getvalue()


def write_csv_gz(rows: list[dict]) -> int:
    text = slim_csv_text(rows)
    with gzip.open(CSV_GZ, "wb", compresslevel=9) as f:
        f.write(text.encode("utf-8"))
    return len(text.encode("utf-8"))


def main() -> None:
    if not CSV_FILE.exists():
        print(f"missing {CSV_FILE}; run `make download` first", file=sys.stderr)
        sys.exit(1)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    rows = load_rows()
    print(f"loaded {len(rows)} confirmed rows", file=sys.stderr)
    stats = build_stats(rows)
    STATS_FILE.write_text(
        json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    raw_bytes = write_csv_gz(rows)
    gz_bytes = CSV_GZ.stat().st_size
    total_pln = stats["totals"]["total_grosze"] / GROSZE_PER_PLN
    print(f"wrote {STATS_FILE}")
    print(
        f"wrote {CSV_GZ} ({gz_bytes / 1_048_576:.2f} MiB gz, "
        f"{raw_bytes / 1_048_576:.2f} MiB raw, "
        f"ratio {gz_bytes / raw_bytes:.2%})"
    )
    print(
        f"sum: {total_pln:,.2f} PLN over {stats['totals']['donations_count']} donations"
    )


if __name__ == "__main__":
    main()
