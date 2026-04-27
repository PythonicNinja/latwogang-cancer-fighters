#!/usr/bin/env -S uv run --no-config --quiet --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Aggregate payments.csv into web/data/stats.json + copy of CSV for the dashboard."""

from __future__ import annotations

import csv
import json
import shutil
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).parent
CSV_FILE = ROOT / "payments.csv"
WEB_DIR = ROOT / "web"
DATA_DIR = WEB_DIR / "data"
STATS_FILE = DATA_DIR / "stats.json"
CSV_COPY = DATA_DIR / "payments.csv"

GROSZE_PER_PLN = 100
TOP_N = 20
LONGEST_COMMENTS_N = 12

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
        amount_grosze = round(float(r["amount"]))
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
    for r in rows:
        at = r["at"]
        if len(at) < 13:
            continue
        try:
            counts[int(at[11:13])] += 1
        except ValueError:
            continue
    return [{"hour": h, "count": counts.get(h, 0)} for h in range(24)]


def by_amount_bucket(rows: list[dict]) -> list[dict]:
    out = []
    for label, lo, hi in AMOUNT_BUCKETS:
        n = sum(1 for r in rows if lo <= r["amount_grosze"] < hi)
        out.append({"range": label, "count": n})
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


def build_stats(rows: list[dict]) -> dict:
    donors = aggregate_by_donor(rows)
    companies = [d for d in donors if d["company"]]
    individuals = [d for d in donors if not d["company"]]
    biggest = max(rows, key=lambda r: r["amount_grosze"]) if rows else None
    return {
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
        "by_amount_bucket": by_amount_bucket(rows),
        "comments": comments_stats(rows),
    }


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
    shutil.copyfile(CSV_FILE, CSV_COPY)
    total_pln = stats["totals"]["total_grosze"] / GROSZE_PER_PLN
    print(f"wrote {STATS_FILE}")
    print(f"copied csv -> {CSV_COPY}")
    print(f"sum: {total_pln:,.2f} PLN over {stats['totals']['donations_count']} donations")


if __name__ == "__main__":
    main()
