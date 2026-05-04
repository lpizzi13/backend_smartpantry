#!/usr/bin/env python3
from __future__ import annotations

import argparse
import random
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

import firebase_admin
from firebase_admin import credentials, firestore

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from home_service import HomeError, HomeService


MEAL_TYPES: Tuple[str, ...] = ("breakfast", "lunch", "dinner", "snacks")


FOODS_BY_MEAL: Dict[str, List[Dict[str, Any]]] = {
    "breakfast": [
        {
            "name": "Fiocchi d'avena",
            "kcal": 389,
            "carbs": 66.3,
            "protein": 16.9,
            "fat": 6.9,
            "grams_range": (35, 90),
        },
        {
            "name": "Yogurt greco 0%",
            "kcal": 59,
            "carbs": 3.6,
            "protein": 10.0,
            "fat": 0.4,
            "grams_range": (100, 250),
        },
        {
            "name": "Latte parzialmente scremato",
            "kcal": 47,
            "carbs": 4.9,
            "protein": 3.4,
            "fat": 1.6,
            "grams_range": (150, 300),
        },
        {
            "name": "Banana",
            "kcal": 89,
            "carbs": 22.8,
            "protein": 1.1,
            "fat": 0.3,
            "grams_range": (90, 180),
        },
        {
            "name": "Pane integrale",
            "kcal": 247,
            "carbs": 41.0,
            "protein": 13.0,
            "fat": 4.2,
            "grams_range": (30, 90),
        },
        {
            "name": "Uova",
            "kcal": 143,
            "carbs": 0.7,
            "protein": 12.6,
            "fat": 9.5,
            "grams_range": (90, 180),
        },
        {
            "name": "Burro di arachidi",
            "kcal": 588,
            "carbs": 20.0,
            "protein": 25.0,
            "fat": 50.0,
            "grams_range": (10, 30),
        },
    ],
    "lunch": [
        {
            "name": "Riso basmati cotto",
            "kcal": 130,
            "carbs": 28.2,
            "protein": 2.7,
            "fat": 0.3,
            "grams_range": (120, 280),
        },
        {
            "name": "Pasta cotta",
            "kcal": 158,
            "carbs": 31.0,
            "protein": 5.8,
            "fat": 0.9,
            "grams_range": (120, 260),
        },
        {
            "name": "Petto di pollo",
            "kcal": 165,
            "carbs": 0.0,
            "protein": 31.0,
            "fat": 3.6,
            "grams_range": (120, 260),
        },
        {
            "name": "Tonno al naturale",
            "kcal": 116,
            "carbs": 0.0,
            "protein": 26.0,
            "fat": 1.0,
            "grams_range": (100, 220),
        },
        {
            "name": "Olio extravergine",
            "kcal": 884,
            "carbs": 0.0,
            "protein": 0.0,
            "fat": 100.0,
            "grams_range": (5, 15),
        },
        {
            "name": "Pane integrale",
            "kcal": 247,
            "carbs": 41.0,
            "protein": 13.0,
            "fat": 4.2,
            "grams_range": (30, 100),
        },
        {
            "name": "Insalata mista",
            "kcal": 20,
            "carbs": 3.0,
            "protein": 1.3,
            "fat": 0.2,
            "grams_range": (80, 200),
        },
    ],
    "dinner": [
        {
            "name": "Salmone",
            "kcal": 208,
            "carbs": 0.0,
            "protein": 20.0,
            "fat": 13.0,
            "grams_range": (120, 240),
        },
        {
            "name": "Tacchino",
            "kcal": 135,
            "carbs": 0.0,
            "protein": 29.0,
            "fat": 1.0,
            "grams_range": (120, 260),
        },
        {
            "name": "Patate lesse",
            "kcal": 87,
            "carbs": 20.1,
            "protein": 1.9,
            "fat": 0.1,
            "grams_range": (120, 320),
        },
        {
            "name": "Lenticchie cotte",
            "kcal": 116,
            "carbs": 20.1,
            "protein": 9.0,
            "fat": 0.4,
            "grams_range": (100, 260),
        },
        {
            "name": "Riso basmati cotto",
            "kcal": 130,
            "carbs": 28.2,
            "protein": 2.7,
            "fat": 0.3,
            "grams_range": (100, 230),
        },
        {
            "name": "Verdure grigliate",
            "kcal": 45,
            "carbs": 8.0,
            "protein": 2.0,
            "fat": 0.6,
            "grams_range": (120, 260),
        },
        {
            "name": "Olio extravergine",
            "kcal": 884,
            "carbs": 0.0,
            "protein": 0.0,
            "fat": 100.0,
            "grams_range": (5, 15),
        },
    ],
    "snacks": [
        {
            "name": "Mandorle",
            "kcal": 579,
            "carbs": 21.6,
            "protein": 21.2,
            "fat": 49.9,
            "grams_range": (10, 35),
        },
        {
            "name": "Mela",
            "kcal": 52,
            "carbs": 13.8,
            "protein": 0.3,
            "fat": 0.2,
            "grams_range": (120, 220),
        },
        {
            "name": "Barretta proteica",
            "kcal": 370,
            "carbs": 35.0,
            "protein": 33.0,
            "fat": 11.0,
            "grams_range": (35, 65),
        },
        {
            "name": "Cioccolato fondente",
            "kcal": 546,
            "carbs": 61.0,
            "protein": 4.9,
            "fat": 31.0,
            "grams_range": (10, 30),
        },
        {
            "name": "Yogurt greco 0%",
            "kcal": 59,
            "carbs": 3.6,
            "protein": 10.0,
            "fat": 0.4,
            "grams_range": (120, 220),
        },
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Popola users/{uid}/home simulando una dieta giornaliera per un intervallo "
            "di date e aggiornando anche weeklyStats/monthlyStats."
        )
    )
    parser.add_argument("--uid", required=True, help="UID dell'utente Firestore")
    parser.add_argument(
        "--days",
        type=int,
        default=365,
        help="Numero di giorni da simulare (default: 365)",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Data inizio (YYYY-MM-DD). Se omessa, parte da end-date - days + 1.",
    )
    parser.add_argument(
        "--end-date",
        default=date.today().strftime("%Y-%m-%d"),
        help="Data fine (YYYY-MM-DD), usata solo se start-date non è impostata.",
    )
    parser.add_argument(
        "--skip-day-probability",
        type=float,
        default=0.08,
        help="Probabilità di saltare un giorno (default: 0.08).",
    )
    parser.add_argument(
        "--overwrite-days",
        action="store_true",
        help="Se presente, rimpiazza i giorni già esistenti nell'intervallo.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=1337,
        help="Seed random per risultati riproducibili (default: 1337).",
    )
    parser.add_argument(
        "--service-account",
        default="serviceAccountKey.json",
        help="Path al JSON del service account Firebase.",
    )
    return parser.parse_args()


def parse_date(raw: str, arg_name: str) -> date:
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{arg_name} non valida: atteso formato YYYY-MM-DD") from exc


def init_home_service(service_account_path: str) -> HomeService:
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

    db = firestore.client()
    return HomeService(db)


def choose_entry_count_for_meal(meal_type: str, rng: random.Random) -> int:
    if meal_type == "breakfast":
        return rng.choices([1, 2, 3], weights=[0.2, 0.6, 0.2], k=1)[0]
    if meal_type == "lunch":
        return rng.choices([1, 2, 3], weights=[0.15, 0.65, 0.2], k=1)[0]
    if meal_type == "dinner":
        return rng.choices([1, 2, 3], weights=[0.2, 0.6, 0.2], k=1)[0]
    return rng.choices([0, 1, 2], weights=[0.35, 0.45, 0.2], k=1)[0]


def scale_nutrients(food: Dict[str, Any], grams: float) -> Dict[str, float]:
    factor = grams / 100.0
    return {
        "kcal": round(float(food["kcal"]) * factor, 3),
        "carbs": round(float(food["carbs"]) * factor, 3),
        "protein": round(float(food["protein"]) * factor, 3),
        "fat": round(float(food["fat"]) * factor, 3),
    }


def create_day_entries(
    home_service: HomeService, uid: str, day_key: str, rng: random.Random
) -> int:
    created_entries = 0
    for meal_type in MEAL_TYPES:
        entries_to_create = choose_entry_count_for_meal(meal_type, rng)
        if entries_to_create <= 0:
            continue

        foods_pool = FOODS_BY_MEAL[meal_type]
        for _ in range(entries_to_create):
            selected_food = rng.choice(foods_pool)
            grams_min, grams_max = selected_food["grams_range"]
            grams = round(rng.uniform(float(grams_min), float(grams_max)), 1)
            nutrients = scale_nutrients(selected_food, grams)

            home_service.add_entry(
                uid=uid,
                date_key=day_key,
                open_food_facts_id=None,
                meal_type=meal_type,
                source="manual",
                product_name=selected_food["name"],
                grams=grams,
                nutrients=nutrients,
            )
            created_entries += 1

    return created_entries


def get_day_or_none(home_service: HomeService, uid: str, day_key: str) -> Dict[str, Any] | None:
    try:
        day_data = home_service.get_day(uid=uid, date_key=day_key)
        if int(day_data.get("entriesCount", 0) or 0) <= 0:
            return None
        return day_data
    except HomeError as exc:
        if exc.status_code == 404:
            return None
        raise


def clear_existing_day(home_service: HomeService, uid: str, day_key: str) -> int:
    day_data = get_day_or_none(home_service, uid, day_key)
    if not day_data:
        return 0

    deleted_entries = 0
    for meal_type in MEAL_TYPES:
        for entry in day_data.get("meals", {}).get(meal_type, []):
            entry_id = entry.get("openFoodFactsId")
            if not entry_id:
                continue
            home_service.delete_entry(uid=uid, date_key=day_key, open_food_facts_id=entry_id)
            deleted_entries += 1
    return deleted_entries


def main() -> None:
    args = parse_args()

    if args.days <= 0:
        raise ValueError("--days deve essere > 0")
    if not 0.0 <= args.skip_day_probability <= 1.0:
        raise ValueError("--skip-day-probability deve essere tra 0 e 1")

    if args.start_date:
        start_day = parse_date(args.start_date, "--start-date")
        end_day = start_day + timedelta(days=args.days - 1)
    else:
        end_day = parse_date(args.end_date, "--end-date")
        start_day = end_day - timedelta(days=args.days - 1)

    home_service = init_home_service(args.service_account)
    rng = random.Random(args.seed)

    simulated_days = 0
    skipped_rest_days = 0
    skipped_existing_days = 0
    overwritten_days = 0
    created_entries_total = 0

    print(
        f"Seeding /home per uid={args.uid} dal {start_day} al {end_day} "
        f"({args.days} giorni, seed={args.seed})"
    )

    for day_index in range(args.days):
        current_day = start_day + timedelta(days=day_index)
        day_key = current_day.strftime("%Y-%m-%d")

        if rng.random() < args.skip_day_probability:
            skipped_rest_days += 1
            continue

        existing_day = get_day_or_none(home_service, args.uid, day_key)
        if existing_day and not args.overwrite_days:
            skipped_existing_days += 1
            continue
        if existing_day and args.overwrite_days:
            clear_existing_day(home_service, args.uid, day_key)
            overwritten_days += 1

        created_entries = create_day_entries(home_service, args.uid, day_key, rng)
        if created_entries > 0:
            simulated_days += 1
            created_entries_total += created_entries

        if (day_index + 1) % 30 == 0 or day_index == args.days - 1:
            print(
                f"[{day_index + 1}/{args.days}] giorni simulati={simulated_days}, "
                f"entry create={created_entries_total}"
            )

    print("Completato.")
    print(
        "Riepilogo: "
        f"simulatedDays={simulated_days}, "
        f"createdEntries={created_entries_total}, "
        f"skippedRestDays={skipped_rest_days}, "
        f"skippedExistingDays={skipped_existing_days}, "
        f"overwrittenDays={overwritten_days}"
    )


if __name__ == "__main__":
    main()
