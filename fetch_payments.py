#!/usr/bin/env -S uv run --no-config --quiet --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["httpx"]
# ///
"""Fetch all payments for a siepomaga.pl fundraise via cursor pagination.

Idempotent: rerun appends only new rows (deduped by id). Streams each page
straight to CSV and flushes, so the file is always crawlable mid-run.
"""

from __future__ import annotations

import csv
import sys
import time
from pathlib import Path

import httpx

API_URL = "https://www.siepomaga.pl/api/v1/payments"
TARGET_TYPE = "Fundraise"
TARGET_ID = "LZSw1Ox"
SORT_BY = "biggest"
LOCALE = "pl"
OUT_DIR = Path(__file__).parent
CSV_FILE = OUT_DIR / "payments.csv"
SLEEP_SEC = 0.01

CSV_FIELDS = [
    "id",
    "amount",
    "currency",
    "comment_text",
    "state",
    "state_changed_at",
    "constant_help",
    "payments_count",
    "highlighted",
    "payer_id",
    "payer_name",
    "payer_company",
    "payer_url",
    "payer_avatar_url",
]


def build_params(after_id: str | None, after_value: str | None) -> dict[str, str]:
    params = {
        "target_type": TARGET_TYPE,
        "target_id": TARGET_ID,
        "sort_by": SORT_BY,
        "locale": LOCALE,
    }
    if after_id and after_value:
        params["after_id"] = after_id
        params["after_value"] = after_value
    return params


def fetch_page(
    client: httpx.Client, after_id: str | None, after_value: str | None
) -> list[dict]:
    r = client.get(API_URL, params=build_params(after_id, after_value), timeout=30.0)
    r.raise_for_status()
    return r.json().get("data", [])


def flatten(item: dict) -> dict:
    payer = item.get("payer") or {}
    return {
        "id": item.get("id"),
        "amount": item.get("amount"),
        "currency": item.get("currency"),
        "comment_text": item.get("comment_text"),
        "state": item.get("state"),
        "state_changed_at": item.get("state_changed_at"),
        "constant_help": item.get("constant_help"),
        "payments_count": item.get("payments_count"),
        "highlighted": item.get("highlighted"),
        "payer_id": payer.get("id"),
        "payer_name": payer.get("name"),
        "payer_company": payer.get("company"),
        "payer_url": payer.get("url"),
        "payer_avatar_url": payer.get("avatar_url"),
    }


def load_seen_ids() -> set[str]:
    if not CSV_FILE.exists() or CSV_FILE.stat().st_size == 0:
        return set()
    with CSV_FILE.open(newline="", encoding="utf-8") as f:
        return {row["id"] for row in csv.DictReader(f) if row.get("id")}


def crawl() -> int:
    seen = load_seen_ids()
    initial = len(seen)
    file_exists = CSV_FILE.exists() and CSV_FILE.stat().st_size > 0
    mode = "a" if file_exists else "w"

    with (
        CSV_FILE.open(mode, newline="", encoding="utf-8") as fp,
        httpx.Client(headers={"Accept": "application/json"}) as client,
    ):
        writer = csv.DictWriter(fp, fieldnames=CSV_FIELDS)
        if not file_exists:
            writer.writeheader()
            fp.flush()

        after_id: str | None = None
        after_value: str | None = None
        page = 0

        while True:
            page += 1
            items = fetch_page(client, after_id, after_value)
            if not items:
                print(f"page {page}: empty, stop", file=sys.stderr)
                break

            new_items = [it for it in items if it["id"] not in seen]
            for it in new_items:
                writer.writerow(flatten(it))
                seen.add(it["id"])
            fp.flush()

            last = items[-1]
            after_id = last["id"]
            after_value = str(last["amount"])
            print(
                f"page {page}: {len(items)} rows ({len(new_items)} new) "
                f"total={len(seen)} cursor=({after_id},{after_value})",
                file=sys.stderr,
            )
            time.sleep(SLEEP_SEC)

    return len(seen) - initial


def main() -> None:
    added = crawl()
    print(f"done. +{added} new rows in {CSV_FILE}")


if __name__ == "__main__":
    main()
