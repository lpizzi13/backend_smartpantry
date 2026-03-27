from __future__ import annotations

from datetime import datetime
import math
import uuid
from typing import Any, Callable, Dict, List, Tuple

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
        target_entry_ref = day_ref.collection(validated_meal_type).document(resolved_id)

        def _tx(transaction: Any) -> Dict[str, Any]:
            existing_matches = self._find_entries_by_id(
                day_ref=day_ref,
                open_food_facts_id=resolved_id,
                transaction=transaction,
            )
            if len(existing_matches) > 1:
                raise HomeError(
                    "Dati Home incoerenti: entry duplicata su pasti multipli",
                    status_code=500,
                )

            existing_match = existing_matches[0] if existing_matches else None
            existing_nutrients = (
                existing_match["entry"]["nutrients"] if existing_match else None
            )
            existing_ref = existing_match["ref"] if existing_match else None
            existing_meal = existing_match["mealType"] if existing_match else None

            totals, entries_count = self._recalculate_day_state(
                day_ref=day_ref,
                transaction=transaction,
            )

            if existing_nutrients is None:
                entries_count += 1
            else:
                totals = self._subtract_nutrients(totals, existing_nutrients)
            totals = self._add_nutrients(totals, validated_nutrients)

            entry_payload = {
                "openFoodFactsId": resolved_id,
                "source": validated_source,
                "productName": validated_product_name,
                "grams": validated_grams,
                "nutrients": validated_nutrients,
            }
            day_payload = self._build_day_payload(
                date_key=validated_date_key,
                totals=totals,
                entries_count=entries_count,
            )

            if existing_ref is not None and existing_meal != validated_meal_type:
                transaction.delete(existing_ref)
            transaction.set(target_entry_ref, entry_payload)
            transaction.set(day_ref, day_payload)
            return day_payload

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

        day_snapshot = day_ref.get()
        meals: Dict[str, List[Dict[str, Any]]] = {meal: [] for meal in MEAL_TYPES}
        totals = self._zero_totals()
        entries_count = 0

        for meal in MEAL_TYPES:
            docs = self._stream_entries(
                entries_ref=day_ref.collection(meal),
                transaction=None,
            )
            for doc in docs:
                entry = self._parse_stored_entry(
                    doc_id=getattr(doc, "id", ""),
                    payload=doc.to_dict() or {},
                    expected_meal_type=meal,
                )
                meals[meal].append(entry)
                totals = self._add_nutrients(totals, entry["nutrients"])
                entries_count += 1

        if not day_snapshot.exists and entries_count == 0:
            raise HomeError("Giorno non trovato", status_code=404)

        return {
            "dateKey": validated_date_key,
            "totals": totals,
            "entriesCount": entries_count,
            "meals": meals,
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
        target_entry_ref = day_ref.collection(validated_meal_type).document(validated_id)

        def _tx(transaction: Any) -> Dict[str, Any]:
            matches = self._find_entries_by_id(
                day_ref=day_ref,
                open_food_facts_id=validated_id,
                transaction=transaction,
            )
            if not matches:
                raise HomeError("Entry non trovata", status_code=404)
            if len(matches) > 1:
                raise HomeError(
                    "Dati Home incoerenti: entry duplicata su pasti multipli",
                    status_code=500,
                )

            existing_match = matches[0]
            existing_entry = existing_match["entry"]
            existing_ref = existing_match["ref"]
            existing_meal = existing_match["mealType"]

            totals, entries_count = self._recalculate_day_state(
                day_ref=day_ref,
                transaction=transaction,
            )
            totals = self._subtract_nutrients(totals, existing_entry["nutrients"])
            totals = self._add_nutrients(totals, validated_nutrients)

            updated_payload = {
                "openFoodFactsId": validated_id,
                "source": existing_entry["source"],
                "productName": existing_entry["productName"],
                "grams": validated_grams,
                "nutrients": validated_nutrients,
            }
            day_payload = self._build_day_payload(
                date_key=validated_date_key,
                totals=totals,
                entries_count=entries_count,
            )

            if existing_meal != validated_meal_type:
                transaction.delete(existing_ref)
            transaction.set(target_entry_ref, updated_payload)
            transaction.set(day_ref, day_payload)
            return day_payload

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
            matches = self._find_entries_by_id(
                day_ref=day_ref,
                open_food_facts_id=validated_id,
                transaction=transaction,
            )
            if not matches:
                raise HomeError("Entry non trovata", status_code=404)
            if len(matches) > 1:
                raise HomeError(
                    "Dati Home incoerenti: entry duplicata su pasti multipli",
                    status_code=500,
                )

            existing_match = matches[0]
            existing_entry = existing_match["entry"]
            existing_ref = existing_match["ref"]

            totals, entries_count = self._recalculate_day_state(
                day_ref=day_ref,
                transaction=transaction,
            )
            totals = self._subtract_nutrients(totals, existing_entry["nutrients"])
            entries_count = max(entries_count - 1, 0)

            day_payload = self._build_day_payload(
                date_key=validated_date_key,
                totals=totals,
                entries_count=entries_count,
            )
            transaction.delete(existing_ref)
            transaction.set(day_ref, day_payload)
            return day_payload

        day_payload = self._run_transaction(_tx)
        return {
            "dateKey": day_payload["dateKey"],
            "totals": day_payload["totals"],
            "entriesCount": day_payload["entriesCount"],
        }

    def _find_entries_by_id(
        self, day_ref: Any, open_food_facts_id: str, transaction: Any
    ) -> List[Dict[str, Any]]:
        matches: List[Dict[str, Any]] = []
        for meal in MEAL_TYPES:
            entry_ref = day_ref.collection(meal).document(open_food_facts_id)
            snapshot = entry_ref.get(transaction=transaction)
            if not snapshot.exists:
                continue
            parsed_entry = self._parse_stored_entry(
                doc_id=open_food_facts_id,
                payload=snapshot.to_dict() or {},
                expected_meal_type=meal,
            )
            matches.append({"mealType": meal, "ref": entry_ref, "entry": parsed_entry})
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

    def _recalculate_day_state(
        self, day_ref: Any, transaction: Any
    ) -> Tuple[Dict[str, float], int]:
        totals = self._zero_totals()
        entries_count = 0
        for meal in MEAL_TYPES:
            docs = self._stream_entries(
                entries_ref=day_ref.collection(meal),
                transaction=transaction,
            )
            for doc in docs:
                entry = self._parse_stored_entry(
                    doc_id=getattr(doc, "id", ""),
                    payload=doc.to_dict() or {},
                    expected_meal_type=meal,
                )
                totals = self._add_nutrients(totals, entry["nutrients"])
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

    @staticmethod
    def _zero_totals() -> Dict[str, float]:
        return {"kcal": 0.0, "carbs": 0.0, "protein": 0.0, "fat": 0.0}

    def _build_day_payload(
        self, date_key: str, totals: Dict[str, float], entries_count: int
    ) -> Dict[str, Any]:
        return {
            "dateKey": date_key,
            "totals": {
                "kcal": round(float(totals["kcal"]), 3),
                "carbs": round(float(totals["carbs"]), 3),
                "protein": round(float(totals["protein"]), 3),
                "fat": round(float(totals["fat"]), 3),
            },
            "entriesCount": int(entries_count),
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
            raise HomeError("productName deve essere una stringa", status_code=status_code)
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

    @staticmethod
    def _add_nutrients(
        totals: Dict[str, float], nutrients: Dict[str, float]
    ) -> Dict[str, float]:
        merged = dict(totals)
        for key in NUTRIENT_KEYS:
            merged[key] = round(
                float(merged.get(key, 0.0)) + float(nutrients.get(key, 0.0)), 3
            )
        return merged

    @staticmethod
    def _subtract_nutrients(
        totals: Dict[str, float], nutrients: Dict[str, float]
    ) -> Dict[str, float]:
        merged = dict(totals)
        for key in NUTRIENT_KEYS:
            value = float(merged.get(key, 0.0)) - float(nutrients.get(key, 0.0))
            if abs(value) < 1e-9:
                value = 0.0
            merged[key] = round(max(value, 0.0), 3)
        return merged
