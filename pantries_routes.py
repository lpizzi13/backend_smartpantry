from datetime import datetime
import json
from typing import Any, Dict, Optional, Tuple

from flask import Blueprint, jsonify, request
from google.api_core import exceptions as gcloud_exceptions

from pantries_service import PantriesError, PantriesService


def create_pantries_blueprint(db: Any) -> Blueprint:
    pantries_bp = Blueprint("pantries", __name__)
    pantries_service = PantriesService(db)

    @pantries_bp.route("/pantry/search", methods=["GET"])
    def search_items() -> Tuple[Any, int]:
        query = request.args.get("q")
        limit = request.args.get("limit", "15")
        lang = request.args.get("lang", "it")
        similar_raw = request.args.get("similar", "true")
        _log_backend(
            "IN",
            "/pantry/search",
            {"q": query, "limit": limit, "lang": lang, "similar": similar_raw},
        )
        try:
            similar = _parse_bool_query_param(
                similar_raw, "similar"
            )
            search_payload = pantries_service.search_products(
                query=query,
                similar=similar,
                limit=limit,
                lang=lang,
            )
            response_payload = {
                "status": "success",
                "query": search_payload["query"],
                "products": search_payload["products"],
                "recommended": search_payload["recommended"],
                # Backward compatibility with existing clients.
                "results": search_payload["products"],
            }
            _log_backend(
                "OUT",
                "/pantry/search",
                {
                    "status": "success",
                    "query": response_payload["query"],
                    "productsCount": len(response_payload["products"]),
                    "recommendedCount": len(response_payload["recommended"]),
                    "productsPreview": [
                        _compact_product_payload(p)
                        for p in response_payload["products"][:3]
                    ],
                },
            )
            return (
                jsonify(response_payload),
                200,
            )
        except Exception as exc:
            return _handle_error(exc)

    @pantries_bp.route("/pantry/barcode/<barcode>", methods=["GET"])
    def get_product_by_barcode(barcode: str) -> Tuple[Any, int]:
        _log_backend("IN", "/pantry/barcode", {"barcode": barcode})
        try:
            result = pantries_service.get_open_food_facts_product(barcode)
            nutrient_payload = _build_client_nutrient_payload(result.get("nutrients"))
            compatible_product = {
                # Legacy fields expected by the Android client.
                "code": result.get("openFoodFactsId", ""),
                "product_name": result.get("productName", ""),
                # Current backend fields.
                "openFoodFactsId": result.get("openFoodFactsId", ""),
                "productName": result.get("productName", ""),
                "brands": result.get("brands", ""),
                "imageUrl": result.get("imageUrl", ""),
                "nutrients": nutrient_payload["nutrients"],
                "kcal": nutrient_payload["kcal"],
                "carbs": nutrient_payload["carbs"],
                "fat": nutrient_payload["fat"],
                "prot": nutrient_payload["prot"],
                "protein": nutrient_payload["protein"],
                "nutriments": nutrient_payload["nutriments"],
            }
            response_payload = {
                # Keep numeric status for backward compatibility with
                # clients that deserialize OFF-like payloads.
                "status": 1,
                "statusText": "success",
                "product": compatible_product,
            }
            _log_backend(
                "OUT",
                "/pantry/barcode",
                {
                    "status": response_payload["status"],
                    "barcode": barcode,
                    "product": _compact_product_payload(compatible_product),
                },
            )
            return (
                jsonify(response_payload),
                200,
            )
        except Exception as exc:
            return _handle_error(exc)

    @pantries_bp.route("/pantry/add", methods=["POST"])
    def add_item() -> Tuple[Any, int]:
        payload = _get_json_payload()
        _log_backend("IN", "/pantry/add", _compact_request_payload(payload))
        try:
            quantity = payload.get("quantity")
            quantity_delta = payload.get("quantityDelta")
            if quantity is not None and quantity_delta is not None:
                raise PantriesError(
                    "Usa quantity oppure quantityDelta, non entrambi",
                    status_code=400,
                )

            if quantity is not None:
                result = pantries_service.set_item_quantity(
                    uid=payload.get("uid"),
                    open_food_facts_id=payload.get("openFoodFactsId"),
                    quantity=quantity,
                    product_name=payload.get("productName"),
                    nutrients=_extract_nutrients_payload(payload),
                    allow_zero=False,
                )
            else:
                result = pantries_service.add_or_upsert_item(
                    uid=payload.get("uid"),
                    open_food_facts_id=payload.get("openFoodFactsId"),
                    quantity_delta=quantity_delta,
                    product_name=payload.get("productName"),
                    nutrients=_extract_nutrients_payload(payload),
                )
            status_code = 201 if result.get("created") else 200
            item_payload = {
                "openFoodFactsId": result["openFoodFactsId"],
                "productName": result["productName"],
                "quantity": result["quantity"],
            }
            if _has_non_zero_nutrients(result.get("nutrients")):
                item_payload["nutrients"] = result["nutrients"]
            _log_backend(
                "OUT",
                "/pantry/add",
                {"status": "success", "item": _compact_product_payload(item_payload)},
            )
            return (jsonify({"status": "success", "item": item_payload}), status_code)
        except Exception as exc:
            return _handle_error(exc)

    @pantries_bp.route("/pantry/quantity", methods=["PATCH"])
    def set_item_quantity() -> Tuple[Any, int]:
        payload = _get_json_payload()
        _log_backend("IN", "/pantry/quantity", _compact_request_payload(payload))
        try:
            result = pantries_service.set_item_quantity(
                uid=payload.get("uid"),
                open_food_facts_id=payload.get("openFoodFactsId"),
                quantity=payload.get("quantity"),
                product_name=payload.get("productName"),
                nutrients=_extract_nutrients_payload(payload),
            )
            item_payload = {
                "openFoodFactsId": result["openFoodFactsId"],
                "productName": result["productName"],
                "quantity": result["quantity"],
                "deleted": bool(result.get("deleted")),
            }
            if _has_non_zero_nutrients(result.get("nutrients")):
                item_payload["nutrients"] = result["nutrients"]
            status_code = 201 if result.get("created") else 200
            _log_backend(
                "OUT",
                "/pantry/quantity",
                {"status": "success", "item": _compact_product_payload(item_payload)},
            )
            return (jsonify({"status": "success", "item": item_payload}), status_code)
        except Exception as exc:
            return _handle_error(exc)

    @pantries_bp.route("/pantry/item", methods=["PATCH"])
    def update_item() -> Tuple[Any, int]:
        payload = _get_json_payload()
        _log_backend("IN", "/pantry/item", _compact_request_payload(payload))
        try:
            result = pantries_service.update_item(
                uid=payload.get("uid"),
                open_food_facts_id=payload.get("openFoodFactsId"),
                quantity=payload.get("quantity"),
                product_name=payload.get("productName"),
                nutrients=_extract_nutrients_payload(payload),
            )
            item_payload = {
                "openFoodFactsId": result["openFoodFactsId"],
                "productName": result["productName"],
                "quantity": result["quantity"],
                "deleted": bool(result.get("deleted")),
            }
            if _has_non_zero_nutrients(result.get("nutrients")):
                item_payload["nutrients"] = result["nutrients"]
            _log_backend(
                "OUT",
                "/pantry/item",
                {"status": "success", "item": _compact_product_payload(item_payload)},
            )
            return (jsonify({"status": "success", "item": item_payload}), 200)
        except Exception as exc:
            return _handle_error(exc)

    @pantries_bp.route("/pantry/decrement", methods=["PATCH"])
    def decrement_item() -> Tuple[Any, int]:
        payload = _get_json_payload()
        try:
            result = pantries_service.decrement_item(
                uid=payload.get("uid"),
                open_food_facts_id=payload.get("openFoodFactsId"),
            )
            item_payload = {
                "openFoodFactsId": result["openFoodFactsId"],
                "productName": result["productName"],
                "quantity": result["quantity"],
                "deleted": result["deleted"],
            }
            if _has_non_zero_nutrients(result.get("nutrients")):
                item_payload["nutrients"] = result["nutrients"]
            return (jsonify({"status": "success", "item": item_payload}), 200)
        except Exception as exc:
            return _handle_error(exc)

    @pantries_bp.route("/pantry/item", methods=["DELETE"])
    def delete_item() -> Tuple[Any, int]:
        payload = _get_json_payload()
        try:
            result = pantries_service.delete_item(
                uid=payload.get("uid"),
                open_food_facts_id=payload.get("openFoodFactsId"),
            )
            return (
                jsonify(
                    {
                        "status": "success",
                        "deleted": result["openFoodFactsId"],
                    }
                ),
                200,
            )
        except Exception as exc:
            return _handle_error(exc)

    @pantries_bp.route("/pantry/<uid>", methods=["GET"])
    def list_items(uid: Optional[str] = None) -> Tuple[Any, int]:
        try:
            resolved_uid = uid if uid is not None else request.args.get("uid")
            items = pantries_service.list_items(uid=resolved_uid)
            return jsonify({"status": "success", "items": items}), 200
        except Exception as exc:
            return _handle_error(exc)

    return pantries_bp


def _get_json_payload() -> Dict[str, Any]:
    payload = request.get_json(silent=True)
    if isinstance(payload, dict):
        return payload
    return {}


def _extract_nutrients_payload(payload: Dict[str, Any]) -> Any:
    nutrients: Dict[str, Any] = {}
    nested = payload.get("nutrients")
    if isinstance(nested, dict):
        nutrients.update(nested)

    for key in ("kcal", "carbs", "fat", "prot", "protein"):
        if key in payload:
            nutrients[key] = payload.get(key)

    if nutrients:
        return nutrients
    return None


def _has_non_zero_nutrients(nutrients: Any) -> bool:
    if not isinstance(nutrients, dict):
        return False
    for value in nutrients.values():
        try:
            if float(value) > 0:
                return True
        except (TypeError, ValueError):
            continue
    return False


def _build_client_nutrient_payload(raw_nutrients: Any) -> Dict[str, Any]:
    nutrients = raw_nutrients if isinstance(raw_nutrients, dict) else {}
    kcal = _first_numeric(nutrients.get("kcal"))
    carbs = _first_numeric(nutrients.get("carbs"))
    fat = _first_numeric(nutrients.get("fat"))
    protein = _first_numeric(nutrients.get("protein"), nutrients.get("prot"))
    return {
        "kcal": kcal,
        "carbs": carbs,
        "fat": fat,
        "prot": protein,
        "protein": protein,
        "nutrients": {
            "kcal": kcal,
            "carbs": carbs,
            "fat": fat,
            "protein": protein,
        },
        "nutriments": {
            "energy-kcal_100g": kcal,
            "carbohydrates_100g": carbs,
            "fat_100g": fat,
            "proteins_100g": protein,
        },
    }


def _extract_macros(product: Dict[str, Any]) -> Dict[str, float]:
    nutrients = product.get("nutrients") if isinstance(product.get("nutrients"), dict) else {}
    nutriments = product.get("nutriments") if isinstance(product.get("nutriments"), dict) else {}
    kcal = _first_numeric(
        product.get("kcal"),
        nutrients.get("kcal"),
        nutriments.get("energy-kcal_100g"),
        nutriments.get("energy_kcal_100g"),
    )
    carbs = _first_numeric(
        product.get("carbs"),
        nutrients.get("carbs"),
        nutriments.get("carbohydrates_100g"),
    )
    fat = _first_numeric(
        product.get("fat"),
        nutrients.get("fat"),
        nutriments.get("fat_100g"),
    )
    prot = _first_numeric(
        product.get("prot"),
        product.get("protein"),
        nutrients.get("prot"),
        nutrients.get("protein"),
        nutriments.get("proteins_100g"),
    )
    return {"kcal": kcal, "carbs": carbs, "fat": fat, "prot": prot}


def _compact_product_payload(product: Dict[str, Any]) -> Dict[str, Any]:
    macros = _extract_macros(product if isinstance(product, dict) else {})
    return {
        "openFoodFactsId": product.get("openFoodFactsId") or product.get("code"),
        "productName": product.get("productName") or product.get("product_name"),
        "quantity": product.get("quantity"),
        "kcal": macros["kcal"],
        "carbs": macros["carbs"],
        "fat": macros["fat"],
        "prot": macros["prot"],
    }


def _compact_request_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    macros = _extract_macros(payload)
    return {
        "uid": payload.get("uid"),
        "openFoodFactsId": payload.get("openFoodFactsId"),
        "productName": payload.get("productName"),
        "quantity": payload.get("quantity"),
        "quantityDelta": payload.get("quantityDelta"),
        "kcal": macros["kcal"],
        "carbs": macros["carbs"],
        "fat": macros["fat"],
        "prot": macros["prot"],
    }


def _first_numeric(*values: Any) -> float:
    for value in values:
        try:
            parsed = float(value)
            if parsed >= 0:
                return parsed
        except (TypeError, ValueError):
            continue
    return 0.0


def _log_backend(direction: str, endpoint: str, payload: Any) -> None:
    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    try:
        data = json.dumps(payload, ensure_ascii=False, default=str)
    except Exception:
        data = str(payload)
    print(f"[{ts}] [PANTRY][{direction}] {endpoint} {data}", flush=True)


def _handle_error(exc: Exception) -> Tuple[Any, int]:
    _log_backend("ERR", request.path if request else "unknown", {"error": str(exc)})
    if isinstance(exc, PantriesError):
        return jsonify({"status": "error", "error": exc.message}), exc.status_code
    if isinstance(exc, gcloud_exceptions.PermissionDenied):
        return jsonify({"status": "error", "error": "Accesso negato a Firestore"}), 403
    if isinstance(exc, gcloud_exceptions.NotFound):
        return jsonify({"status": "error", "error": "Documento Firestore non trovato"}), 404
    if isinstance(exc, gcloud_exceptions.InvalidArgument):
        return jsonify({"status": "error", "error": "Input Firestore non valido"}), 400
    if isinstance(exc, gcloud_exceptions.FailedPrecondition):
        return jsonify({"status": "error", "error": "Operazione Firestore non consentita"}), 409
    if isinstance(exc, gcloud_exceptions.GoogleAPICallError):
        return jsonify({"status": "error", "error": "Errore temporaneo Firestore"}), 503
    return jsonify({"status": "error", "error": "Errore interno del server"}), 500


def _parse_bool_query_param(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if not isinstance(value, str):
        raise PantriesError(f"{field_name} deve essere true o false", status_code=400)

    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no", ""}:
        return False
    raise PantriesError(f"{field_name} deve essere true o false", status_code=400)
