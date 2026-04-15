from __future__ import annotations

from datetime import datetime, timedelta
import math
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple

from firebase_admin import firestore
from google.api_core import exceptions as gcloud_exceptions


MEAL_TYPES: Tuple[str, ...] = ("breakfast", "lunch", "dinner", "snacks")
SOURCES: Tuple[str, ...] = ("openfoodfacts", "manual")
NUTRIENT_KEYS: Tuple[str, ...] = ("kcal", "carbs", "protein", "fat")
MAX_TRANSACTION_RETRIES = 3


class HomeError(Exception):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class HomeService:
    def __init__(self, db: Any):
        self._db = db

    def add_entry(
        self,
        uid: Any,
        date_key: Any,
        open_food_facts_id: Any,
        meal_type: Any,
        source: Any,
        product_name: Any,
        grams: Any,
        nutrients: Any,
    ) -> Dict[str, Any]:
        validated_uid = self._validate_uid(uid)
        validated_date_key = self._validate_date_key(date_key)
        validated_meal_type = self._validate_meal_type(meal_type)
        validated_source = self._validate_source(source)
        validated_product_name = self._validate_product_name(product_name)
        validated_grams = self._validate_grams(grams)
        validated_nutrients = self._validate_nutrients(nutrients)
        resolved_id = self._resolve_add_entry_id(
            open_food_facts_id=open_food_facts_id,
            source=validated_source,
        )
        day_ref = self._day_doc_ref(validated_uid, validated_date_key)

        def _tx(transaction: Any) -> Dict[str, Any]:
            day_state = self._load_day_state(
                day_ref=day_ref,
                date_key=validated_date_key,
                transaction=transaction,
            )
            meals = self._clone_meals(day_state["meals"])
            previous_totals = day_state["totals"]
            previous_entries_count = day_state["entriesCount"]

            matches = self._find_entry_occurrences(
                meals=meals,
                open_food_facts_id=resolved_id,
            )
            if len(matches) > 1:
                raise HomeError(
                    "Dati Home incoerenti: entry duplicata su pasti multipli",
                    status_code=500,
                )

            insertion_index: Optional[int] = None
            if matches:
                current_meal, index = matches[0]
                meals[current_meal].pop(index)
                if current_meal == validated_meal_type:
                    insertion_index = index

            entry_payload = {
                "openFoodFactsId": resolved_id,
                "source": validated_source,
                "productName": validated_product_name,
                "grams": validated_grams,
                "nutrients": validated_nutrients,
            }

            target_meal_entries = meals[validated_meal_type]
            if insertion_index is not None and insertion_index <= len(target_meal_entries):
                target_meal_entries.insert(insertion_index, entry_payload)
            else:
                target_meal_entries.append(entry_payload)

            return self._persist_day_and_aggregates(
                transaction=transaction,
                uid=validated_uid,
                date_key=validated_date_key,
                day_ref=day_ref,
                previous_totals=previous_totals,
                previous_entries_count=previous_entries_count,
                updated_meals=meals,
            )

        day_payload = self._run_transaction(_tx)
        return {
            "dateKey": day_payload["dateKey"],
            "totals": day_payload["totals"],
            "entriesCount": day_payload["entriesCount"],
        }

    def get_day(self, uid: Any, date_key: Any) -> Dict[str, Any]:
        validated_uid = self._validate_uid(uid)
        validated_date_key = self._validate_date_key(date_key)
        day_ref = self._day_doc_ref(validated_uid, validated_date_key)
        day_state = self._load_day_state(
            day_ref=day_ref,
            date_key=validated_date_key,
            transaction=None,
        )
        if not day_state["exists"]:
            raise HomeError("Giorno non trovato", status_code=404)

        return {
            "dateKey": validated_date_key,
            "totals": day_state["totals"],
            "entriesCount": day_state["entriesCount"],
            "meals": day_state["meals"],
        }

    def patch_entry(
        self,
        uid: Any,
        date_key: Any,
        open_food_facts_id: Any,
        meal_type: Any,
        grams: Any,
        nutrients: Any,
    ) -> Dict[str, Any]:
        validated_uid = self._validate_uid(uid)
        validated_date_key = self._validate_date_key(date_key)
        validated_id = self._validate_open_food_facts_id(
            open_food_facts_id, required=True
        )
        validated_meal_type = self._validate_meal_type(meal_type)
        validated_grams = self._validate_grams(grams)
        validated_nutrients = self._validate_nutrients(nutrients)
        day_ref = self._day_doc_ref(validated_uid, validated_date_key)

        def _tx(transaction: Any) -> Dict[str, Any]:
            day_state = self._load_day_state(
                day_ref=day_ref,
                date_key=validated_date_key,
                transaction=transaction,
            )
            meals = self._clone_meals(day_state["meals"])
            previous_totals = day_state["totals"]
            previous_entries_count = day_state["entriesCount"]

            matches = self._find_entry_occurrences(
                meals=meals,
                open_food_facts_id=validated_id,
            )
            if not matches:
                raise HomeError("Entry non trovata", status_code=404)
            if len(matches) > 1:
                raise HomeError(
                    "Dati Home incoerenti: entry duplicata su pasti multipli",
                    status_code=500,
                )

            existing_meal, existing_index = matches[0]
            existing_entry = meals[existing_meal].pop(existing_index)
            updated_payload = {
                "openFoodFactsId": validated_id,
                "source": existing_entry["source"],
                "productName": existing_entry["productName"],
                "grams": validated_grams,
                "nutrients": validated_nutrients,
            }
            meals[validated_meal_type].append(updated_payload)

            return self._persist_day_and_aggregates(
                transaction=transaction,
                uid=validated_uid,
                date_key=validated_date_key,
                day_ref=day_ref,
                previous_totals=previous_totals,
                previous_entries_count=previous_entries_count,
                updated_meals=meals,
            )

        day_payload = self._run_transaction(_tx)
        return {
            "dateKey": day_payload["dateKey"],
            "totals": day_payload["totals"],
            "entriesCount": day_payload["entriesCount"],
        }

    def delete_entry(
        self, uid: Any, date_key: Any, open_food_facts_id: Any
    ) -> Dict[str, Any]:
        validated_uid = self._validate_uid(uid)
        validated_date_key = self._validate_date_key(date_key)
        validated_id = self._validate_open_food_facts_id(
            open_food_facts_id, required=True
        )
        day_ref = self._day_doc_ref(validated_uid, validated_date_key)

        def _tx(transaction: Any) -> Dict[str, Any]:
            day_state = self._load_day_state(
                day_ref=day_ref,
                date_key=validated_date_key,
                transaction=transaction,
            )
            meals = self._clone_meals(day_state["meals"])
            previous_totals = day_state["totals"]
            previous_entries_count = day_state["entriesCount"]

            matches = self._find_entry_occurrences(
                meals=meals,
                open_food_facts_id=validated_id,
            )
            if not matches:
                raise HomeError("Entry non trovata", status_code=404)
            if len(matches) > 1:
                raise HomeError(
                    "Dati Home incoerenti: entry duplicata su pasti multipli",
                    status_code=500,
                )

            meal_type_found, index = matches[0]
            meals[meal_type_found].pop(index)

            return self._persist_day_and_aggregates(
                transaction=transaction,
                uid=validated_uid,
                date_key=validated_date_key,
                day_ref=day_ref,
                previous_totals=previous_totals,
                previous_entries_count=previous_entries_count,
                updated_meals=meals,
            )

        day_payload = self._run_transaction(_tx)
        return {
            "dateKey": day_payload["dateKey"],
            "totals": day_payload["totals"],
            "entriesCount": day_payload["entriesCount"],
        }

    def _persist_day_and_aggregates(
        self,
        transaction: Any,
        uid: str,
        date_key: str,
        day_ref: Any,
        previous_totals: Dict[str, float],
        previous_entries_count: int,
        updated_meals: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        next_totals, next_entries_count = self._calculate_day_metrics(updated_meals)
        previous_has_entries = previous_entries_count > 0
        next_has_entries = next_entries_count > 0

        delta_totals = self._diff_totals(previous_totals, next_totals)
        delta_entries_count = int(next_entries_count) - int(previous_entries_count)
        delta_days_count = int(next_has_entries) - int(previous_has_entries)

        # Firestore richiede tutte le read prima delle write nella stessa transazione.
        self._update_period_aggregates(
            transaction=transaction,
            uid=uid,
            date_key=date_key,
            delta_totals=delta_totals,
            delta_entries_count=delta_entries_count,
            delta_days_count=delta_days_count,
        )

        if next_has_entries:
            day_payload = self._build_day_payload(
                date_key=date_key,
                meals=updated_meals,
                totals=next_totals,
                entries_count=next_entries_count,
            )
            transaction.set(day_ref, day_payload, merge=True)
        else:
            transaction.delete(day_ref)
            day_payload = self._build_empty_day_payload(date_key=date_key)

        return day_payload

    def _update_period_aggregates(
        self,
        transaction: Any,
        uid: str,
        date_key: str,
        delta_totals: Dict[str, float],
        delta_entries_count: int,
        delta_days_count: int,
    ) -> None:
        if (
            self._is_zero_totals(delta_totals)
            and delta_entries_count == 0
            and delta_days_count == 0
        ):
            return

        week_info = self._week_info(date_key)
        weekly_ref = self._weekly_stats_doc_ref(uid, week_info["weekKey"])
        weekly_snapshot = weekly_ref.get(transaction=transaction)

        month_info = self._month_info(date_key)
        monthly_ref = self._monthly_stats_doc_ref(uid, month_info["monthKey"])
        monthly_snapshot = monthly_ref.get(transaction=transaction)

        self._apply_aggregate_delta(
            transaction=transaction,
            doc_ref=weekly_ref,
            snapshot=weekly_snapshot,
            period_payload={
                "weekKey": week_info["weekKey"],
                "startDateKey": week_info["startDateKey"],
                "endDateKey": week_info["endDateKey"],
            },
            delta_totals=delta_totals,
            delta_entries_count=delta_entries_count,
            delta_days_count=delta_days_count,
        )
        self._apply_aggregate_delta(
            transaction=transaction,
            doc_ref=monthly_ref,
            snapshot=monthly_snapshot,
            period_payload={
                "monthKey": month_info["monthKey"],
                "year": month_info["year"],
                "month": month_info["month"],
            },
            delta_totals=delta_totals,
            delta_entries_count=delta_entries_count,
            delta_days_count=delta_days_count,
        )

    def _apply_aggregate_delta(
        self,
        transaction: Any,
        doc_ref: Any,
        snapshot: Any,
        period_payload: Dict[str, Any],
        delta_totals: Dict[str, float],
        delta_entries_count: int,
        delta_days_count: int,
    ) -> None:
        existing_payload = snapshot.to_dict() if snapshot.exists else {}
        existing_totals = self._extract_totals(existing_payload.get("totals"))
        existing_entries_count = self._coerce_int(existing_payload.get("entriesCount"))
        existing_days_count = self._coerce_int(existing_payload.get("daysCount"))

        updated_totals = self._add_totals(existing_totals, delta_totals)
        updated_entries_count = max(existing_entries_count + int(delta_entries_count), 0)
        updated_days_count = max(existing_days_count + int(delta_days_count), 0)

        if (
            updated_entries_count == 0
            and updated_days_count == 0
            and self._is_zero_totals(updated_totals)
        ):
            if snapshot.exists:
                transaction.delete(doc_ref)
            return

        payload = {
            **period_payload,
            "totals": updated_totals,
            "dailyAverages": self._compute_period_daily_averages(
                totals=updated_totals, period_payload=period_payload
            ),
            "entriesCount": updated_entries_count,
            "daysCount": updated_days_count,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        }
        transaction.set(doc_ref, payload, merge=True)

    def _find_entry_occurrences(
        self, meals: Dict[str, List[Dict[str, Any]]], open_food_facts_id: str
    ) -> List[Tuple[str, int]]:
        matches: List[Tuple[str, int]] = []
        for meal in MEAL_TYPES:
            meal_entries = meals.get(meal, [])
            for index, entry in enumerate(meal_entries):
                entry_id = self._validate_open_food_facts_id(
                    entry.get("openFoodFactsId"),
                    required=True,
                    status_code=500,
                )
                if entry_id == open_food_facts_id:
                    matches.append((meal, index))
        return matches

    def _run_transaction(
        self, callback: Callable[[Any], Dict[str, Any]]
    ) -> Dict[str, Any]:
        for attempt in range(MAX_TRANSACTION_RETRIES):
            transaction = self._db.transaction()
            try:
                use_firestore_transaction = callable(
                    getattr(firestore, "transactional", None)
                ) and hasattr(transaction, "_id")

                if use_firestore_transaction:
                    result = firestore.transactional(callback)(transaction)
                else:
                    result = callback(transaction)
                    if hasattr(transaction, "commit"):
                        transaction.commit()
                return result
            except HomeError:
                self._rollback_quietly(transaction)
                raise
            except gcloud_exceptions.Aborted as exc:
                self._rollback_quietly(transaction)
                if attempt == MAX_TRANSACTION_RETRIES - 1:
                    raise HomeError(
                        "Transazione Firestore fallita", status_code=500
                    ) from exc
            except Exception:
                self._rollback_quietly(transaction)
                raise

        raise HomeError("Transazione Firestore fallita", status_code=500)

    @staticmethod
    def _rollback_quietly(transaction: Any) -> None:
        if hasattr(transaction, "rollback"):
            try:
                transaction.rollback()
            except Exception:
                return

    def _load_day_state(
        self, day_ref: Any, date_key: str, transaction: Any
    ) -> Dict[str, Any]:
        day_snapshot = day_ref.get(transaction=transaction)
        day_exists = bool(day_snapshot.exists)
        raw_payload = day_snapshot.to_dict() if day_exists else {}
        if raw_payload is None:
            raw_payload = {}

        meals = self._parse_embedded_meals(raw_payload)
        if meals is None:
            meals = self._load_legacy_meals(day_ref=day_ref, transaction=transaction)

        totals, entries_count = self._calculate_day_metrics(meals)
        has_legacy_entries = entries_count > 0

        return {
            "exists": day_exists or has_legacy_entries,
            "dateKey": date_key,
            "meals": meals,
            "totals": totals,
            "entriesCount": entries_count,
        }

    def _parse_embedded_meals(
        self, payload: Dict[str, Any]
    ) -> Optional[Dict[str, List[Dict[str, Any]]]]:
        has_meal_field = any(meal in payload for meal in MEAL_TYPES)
        if not has_meal_field:
            return None

        meals: Dict[str, List[Dict[str, Any]]] = self._empty_meals()
        for meal in MEAL_TYPES:
            raw_entries = payload.get(meal, [])
            if raw_entries is None:
                raw_entries = []
            if not isinstance(raw_entries, list):
                raise HomeError(
                    f"Dati Home incoerenti: campo {meal} non valido",
                    status_code=500,
                )
            for item in raw_entries:
                if not isinstance(item, dict):
                    raise HomeError(
                        f"Dati Home incoerenti: entry non valida in {meal}",
                        status_code=500,
                    )
                parsed = self._parse_stored_entry(
                    doc_id=str(item.get("openFoodFactsId") or ""),
                    payload=item,
                    expected_meal_type=meal,
                )
                meals[meal].append(parsed)
        return meals

    def _load_legacy_meals(
        self, day_ref: Any, transaction: Any
    ) -> Dict[str, List[Dict[str, Any]]]:
        meals: Dict[str, List[Dict[str, Any]]] = self._empty_meals()
        for meal in MEAL_TYPES:
            docs = self._stream_entries(
                entries_ref=day_ref.collection(meal),
                transaction=transaction,
            )
            for doc in docs:
                parsed_entry = self._parse_stored_entry(
                    doc_id=getattr(doc, "id", ""),
                    payload=doc.to_dict() or {},
                    expected_meal_type=meal,
                )
                meals[meal].append(parsed_entry)
        return meals

    def _calculate_day_metrics(
        self, meals: Dict[str, List[Dict[str, Any]]]
    ) -> Tuple[Dict[str, float], int]:
        totals = self._zero_totals()
        entries_count = 0
        for meal in MEAL_TYPES:
            meal_entries = meals.get(meal, [])
            for entry in meal_entries:
                totals = self._add_totals(totals, entry["nutrients"])
                entries_count += 1
        return totals, entries_count

    @staticmethod
    def _stream_entries(entries_ref: Any, transaction: Any) -> List[Any]:
        if transaction is not None:
            try:
                return list(entries_ref.stream(transaction=transaction))
            except TypeError:
                pass
        return list(entries_ref.stream())

    def _parse_stored_entry(
        self,
        doc_id: str,
        payload: Dict[str, Any],
        expected_meal_type: str,
    ) -> Dict[str, Any]:
        open_food_facts_id = self._validate_open_food_facts_id(
            payload.get("openFoodFactsId") or doc_id,
            required=True,
            status_code=500,
        )
        self._validate_meal_type(expected_meal_type, status_code=500)
        source = self._validate_source(payload.get("source"), status_code=500)
        product_name = self._validate_product_name(
            payload.get("productName"), status_code=500
        )
        grams = self._validate_grams(payload.get("grams"), status_code=500)
        nutrients = self._validate_nutrients(payload.get("nutrients"), status_code=500)
        return {
            "openFoodFactsId": open_food_facts_id,
            "source": source,
            "productName": product_name,
            "grams": grams,
            "nutrients": nutrients,
        }

    def _build_day_payload(
        self,
        date_key: str,
        meals: Dict[str, List[Dict[str, Any]]],
        totals: Dict[str, float],
        entries_count: int,
    ) -> Dict[str, Any]:
        normalized_meals = self._clone_meals(meals)
        return {
            "dateKey": date_key,
            "entriesCount": int(entries_count),
            "totals": self._normalize_totals(totals),
            "breakfast": normalized_meals["breakfast"],
            "lunch": normalized_meals["lunch"],
            "dinner": normalized_meals["dinner"],
            "snacks": normalized_meals["snacks"],
            "updatedAt": firestore.SERVER_TIMESTAMP,
        }

    def _build_empty_day_payload(self, date_key: str) -> Dict[str, Any]:
        return {
            "dateKey": date_key,
            "entriesCount": 0,
            "totals": self._zero_totals(),
        }

    def _resolve_add_entry_id(self, open_food_facts_id: Any, source: str) -> str:
        normalized_id = self._validate_open_food_facts_id(
            open_food_facts_id, required=False
        )
        if source == "openfoodfacts":
            if not normalized_id:
                raise HomeError("openFoodFactsId obbligatorio per source=openfoodfacts")
            return normalized_id
        if normalized_id:
            return normalized_id
        return f"manual_{uuid.uuid4().hex}"

    def _day_doc_ref(self, uid: str, date_key: str) -> Any:
        return self._db.collection("users").document(uid).collection("home").document(
            date_key
        )

    def _weekly_stats_doc_ref(self, uid: str, week_key: str) -> Any:
        return (
            self._db.collection("users")
            .document(uid)
            .collection("weeklyStats")
            .document(week_key)
        )

    def _monthly_stats_doc_ref(self, uid: str, month_key: str) -> Any:
        return (
            self._db.collection("users")
            .document(uid)
            .collection("monthlyStats")
            .document(month_key)
        )

    @staticmethod
    def _week_info(date_key: str) -> Dict[str, str]:
        current_date = datetime.strptime(date_key, "%Y-%m-%d").date()
        iso_calendar = current_date.isocalendar()
        week_key = f"{iso_calendar.year}-W{iso_calendar.week:02d}"
        start_date = current_date - timedelta(days=current_date.weekday())
        end_date = start_date + timedelta(days=6)
        return {
            "weekKey": week_key,
            "startDateKey": start_date.strftime("%Y-%m-%d"),
            "endDateKey": end_date.strftime("%Y-%m-%d"),
        }

    @staticmethod
    def _month_info(date_key: str) -> Dict[str, Any]:
        current_date = datetime.strptime(date_key, "%Y-%m-%d").date()
        return {
            "monthKey": current_date.strftime("%Y-%m"),
            "year": current_date.year,
            "month": current_date.month,
        }

    @staticmethod
    def _empty_meals() -> Dict[str, List[Dict[str, Any]]]:
        return {meal: [] for meal in MEAL_TYPES}

    def _clone_meals(
        self, meals: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        cloned: Dict[str, List[Dict[str, Any]]] = self._empty_meals()
        for meal in MEAL_TYPES:
            for entry in meals.get(meal, []):
                cloned[meal].append(
                    {
                        "openFoodFactsId": entry["openFoodFactsId"],
                        "source": entry["source"],
                        "productName": entry["productName"],
                        "grams": float(entry["grams"]),
                        "nutrients": self._normalize_totals(entry["nutrients"]),
                    }
                )
        return cloned

    @staticmethod
    def _normalize_totals(totals: Dict[str, Any]) -> Dict[str, float]:
        normalized: Dict[str, float] = {}
        for key in NUTRIENT_KEYS:
            value = float(totals.get(key, 0.0))
            if abs(value) < 1e-9:
                value = 0.0
            normalized[key] = round(max(value, 0.0), 3)
        return normalized

    @staticmethod
    def _extract_totals(raw_totals: Any) -> Dict[str, float]:
        if not isinstance(raw_totals, dict):
            return {"kcal": 0.0, "carbs": 0.0, "protein": 0.0, "fat": 0.0}
        parsed: Dict[str, float] = {}
        for key in NUTRIENT_KEYS:
            raw_value = raw_totals.get(key, 0.0)
            try:
                value = float(raw_value)
            except (TypeError, ValueError):
                value = 0.0
            if not math.isfinite(value):
                value = 0.0
            if abs(value) < 1e-9:
                value = 0.0
            parsed[key] = round(max(value, 0.0), 3)
        return parsed

    @staticmethod
    def _coerce_int(value: Any) -> int:
        if value is None or isinstance(value, bool):
            return 0
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return 0
        return max(parsed, 0)

    @staticmethod
    def _diff_totals(
        previous_totals: Dict[str, float], next_totals: Dict[str, float]
    ) -> Dict[str, float]:
        diff: Dict[str, float] = {}
        for key in NUTRIENT_KEYS:
            value = float(next_totals.get(key, 0.0)) - float(
                previous_totals.get(key, 0.0)
            )
            if abs(value) < 1e-9:
                value = 0.0
            diff[key] = round(value, 3)
        return diff

    @staticmethod
    def _add_totals(
        totals: Dict[str, float], delta: Dict[str, float]
    ) -> Dict[str, float]:
        merged = dict(totals)
        for key in NUTRIENT_KEYS:
            value = float(merged.get(key, 0.0)) + float(delta.get(key, 0.0))
            if abs(value) < 1e-9:
                value = 0.0
            merged[key] = round(max(value, 0.0), 3)
        return merged

    @staticmethod
    def _is_zero_totals(totals: Dict[str, float]) -> bool:
        for key in NUTRIENT_KEYS:
            if abs(float(totals.get(key, 0.0))) >= 1e-9:
                return False
        return True

    @staticmethod
    def _zero_totals() -> Dict[str, float]:
        return {"kcal": 0.0, "carbs": 0.0, "protein": 0.0, "fat": 0.0}

    def _compute_period_daily_averages(
        self, totals: Dict[str, float], period_payload: Dict[str, Any]
    ) -> Dict[str, float]:
        period_days = self._resolve_period_days(period_payload)
        averages: Dict[str, float] = {}
        for key in NUTRIENT_KEYS:
            value = float(totals.get(key, 0.0)) / float(period_days)
            if abs(value) < 1e-9:
                value = 0.0
            averages[key] = round(max(value, 0.0), 3)
        return averages

    @staticmethod
    def _resolve_period_days(period_payload: Dict[str, Any]) -> int:
        if "weekKey" in period_payload:
            return 7

        raw_year = period_payload.get("year")
        raw_month = period_payload.get("month")
        try:
            year = int(raw_year)
            month = int(raw_month)
            if month < 1 or month > 12:
                raise ValueError
            month_start = datetime(year, month, 1).date()
            if month == 12:
                next_month_start = datetime(year + 1, 1, 1).date()
            else:
                next_month_start = datetime(year, month + 1, 1).date()
            return max((next_month_start - month_start).days, 1)
        except (TypeError, ValueError):
            return 30

    @staticmethod
    def _validate_uid(uid: Any) -> str:
        if not isinstance(uid, str) or not uid.strip():
            raise HomeError("uid obbligatorio")
        return uid.strip()

    @staticmethod
    def _validate_date_key(date_key: Any) -> str:
        if not isinstance(date_key, str):
            raise HomeError("dateKey non valido (atteso YYYY-MM-DD)")
        normalized = date_key.strip()
        try:
            parsed = datetime.strptime(normalized, "%Y-%m-%d")
        except ValueError as exc:
            raise HomeError("dateKey non valido (atteso YYYY-MM-DD)") from exc
        if parsed.strftime("%Y-%m-%d") != normalized:
            raise HomeError("dateKey non valido (atteso YYYY-MM-DD)")
        return normalized

    @staticmethod
    def _validate_open_food_facts_id(
        open_food_facts_id: Any,
        required: bool,
        status_code: int = 400,
    ) -> str:
        if open_food_facts_id is None:
            if required:
                raise HomeError("openFoodFactsId obbligatorio", status_code=status_code)
            return ""
        if not isinstance(open_food_facts_id, str):
            raise HomeError(
                "openFoodFactsId deve essere una stringa", status_code=status_code
            )
        normalized = open_food_facts_id.strip()
        if not normalized:
            if required:
                raise HomeError("openFoodFactsId obbligatorio", status_code=status_code)
            return ""
        if "/" in normalized:
            raise HomeError("openFoodFactsId non valido", status_code=status_code)
        return normalized

    @staticmethod
    def _validate_meal_type(meal_type: Any, status_code: int = 400) -> str:
        if not isinstance(meal_type, str):
            raise HomeError("mealType non valido", status_code=status_code)
        normalized = meal_type.strip().lower()
        if normalized == "snack":
            normalized = "snacks"
        if normalized not in MEAL_TYPES:
            raise HomeError("mealType non valido", status_code=status_code)
        return normalized

    @staticmethod
    def _validate_source(source: Any, status_code: int = 400) -> str:
        if not isinstance(source, str):
            raise HomeError("source non valido", status_code=status_code)
        normalized = source.strip().lower()
        if normalized not in SOURCES:
            raise HomeError("source non valido", status_code=status_code)
        return normalized

    @staticmethod
    def _validate_product_name(product_name: Any, status_code: int = 400) -> str:
        if not isinstance(product_name, str):
            raise HomeError(
                "productName deve essere una stringa", status_code=status_code
            )
        return product_name.strip()

    @staticmethod
    def _validate_grams(grams: Any, status_code: int = 400) -> float:
        if grams is None or isinstance(grams, bool):
            raise HomeError("grams deve essere un numero > 0", status_code=status_code)
        try:
            parsed = float(grams)
        except (TypeError, ValueError) as exc:
            raise HomeError("grams deve essere un numero > 0", status_code=status_code) from exc
        if not math.isfinite(parsed) or parsed <= 0:
            raise HomeError("grams deve essere un numero > 0", status_code=status_code)
        return round(parsed, 3)

    def _validate_nutrients(
        self, nutrients: Any, status_code: int = 400
    ) -> Dict[str, float]:
        if not isinstance(nutrients, dict):
            raise HomeError("nutrients deve essere un oggetto", status_code=status_code)

        parsed: Dict[str, float] = {}
        for key in NUTRIENT_KEYS:
            raw_value = nutrients.get(key)
            if raw_value is None or isinstance(raw_value, bool):
                raise HomeError(
                    f"nutrients.{key} deve essere un numero >= 0",
                    status_code=status_code,
                )
            try:
                value = float(raw_value)
            except (TypeError, ValueError) as exc:
                raise HomeError(
                    f"nutrients.{key} deve essere un numero >= 0",
                    status_code=status_code,
                ) from exc
            if not math.isfinite(value) or value < 0:
                raise HomeError(
                    f"nutrients.{key} deve essere un numero >= 0",
                    status_code=status_code,
                )
            parsed[key] = round(value, 3)
        return parsed
