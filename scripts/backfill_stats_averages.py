#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

import firebase_admin
from firebase_admin import credentials, firestore


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


NUTRIENT_KEYS: Tuple[str, ...] = ("kcal", "carbs", "protein", "fat")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill campo dailyAverages in users/{uid}/weeklyStats e monthlyStats "
            "a partire dai totals esistenti."
        )
    )
    parser.add_argument("--uid", required=True, help="UID dell'utente da aggiornare")
    parser.add_argument(
        "--service-account",
        default="serviceAccountKey.json",
        help="Path al JSON del service account Firebase.",
    )
    return parser.parse_args()


def init_db(service_account_path: str):
    credentials_path = Path(service_account_path)
    if not credentials_path.exists():
        raise FileNotFoundError(
            f"Service account non trovato: {credentials_path.resolve()}"
        )

    try:
        firebase_admin.get_app()
    except ValueError:
        cred = credentials.Certificate(str(credentials_path))
        firebase_admin.initialize_app(cred)
    return firestore.client()


def parse_totals(raw_totals: Any) -> Dict[str, float]:
    if not isinstance(raw_totals, dict):
        raw_totals = {}
    parsed: Dict[str, float] = {}
    for key in NUTRIENT_KEYS:
        raw_value = raw_totals.get(key, 0.0)
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            value = 0.0
        parsed[key] = round(max(value, 0.0), 3)
    return parsed


def compute_averages(totals: Dict[str, float], period_days: int) -> Dict[str, float]:
    safe_days = max(int(period_days), 1)
    averages: Dict[str, float] = {}
    for key in NUTRIENT_KEYS:
        value = float(totals.get(key, 0.0)) / float(safe_days)
        averages[key] = round(max(value, 0.0), 3)
    return averages


def days_in_month(year: int, month: int) -> int:
    month_start = datetime(year, month, 1).date()
    if month == 12:
        next_month_start = datetime(year + 1, 1, 1).date()
    else:
        next_month_start = datetime(year, month + 1, 1).date()
    return max((next_month_start - month_start).days, 1)


def resolve_month_period(payload: Dict[str, Any], doc_id: str) -> int:
    raw_year = payload.get("year")
    raw_month = payload.get("month")

    if raw_year is None or raw_month is None:
        # fallback su monthKey/doc id nel formato YYYY-MM
        month_key = str(payload.get("monthKey") or doc_id)
        if "-" in month_key:
            parts = month_key.split("-")
            if len(parts) >= 2:
                raw_year = raw_year if raw_year is not None else parts[0]
                raw_month = raw_month if raw_month is not None else parts[1]

    try:
        year = int(raw_year)
        month = int(raw_month)
        if month < 1 or month > 12:
            raise ValueError
        return days_in_month(year, month)
    except (TypeError, ValueError):
        return 30


def batched(iterable: Iterable[Any], size: int):
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def main() -> None:
    args = parse_args()
    db = init_db(args.service_account)
    user_ref = db.collection("users").document(args.uid)

    weekly_docs = list(user_ref.collection("weeklyStats").stream())
    monthly_docs = list(user_ref.collection("monthlyStats").stream())

    updates = []

    for snapshot in weekly_docs:
        payload = snapshot.to_dict() or {}
        totals = parse_totals(payload.get("totals"))
        daily_averages = compute_averages(totals, period_days=7)
        updates.append((snapshot.reference, {"dailyAverages": daily_averages}))

    for snapshot in monthly_docs:
        payload = snapshot.to_dict() or {}
        totals = parse_totals(payload.get("totals"))
        period_days = resolve_month_period(payload, snapshot.id)
        daily_averages = compute_averages(totals, period_days=period_days)
        updates.append((snapshot.reference, {"dailyAverages": daily_averages}))

    for chunk in batched(updates, size=400):
        batch = db.batch()
        for doc_ref, update_payload in chunk:
            batch.set(
                doc_ref,
                {**update_payload, "updatedAt": firestore.SERVER_TIMESTAMP},
                merge=True,
            )
        batch.commit()

    print(
        "Backfill completato: "
        f"weeklyStats={len(weekly_docs)}, monthlyStats={len(monthly_docs)}"
    )


if __name__ == "__main__":
    main()
