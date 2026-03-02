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
        try:
            similar = _parse_bool_query_param(
                request.args.get("similar", "false"), "similar"
            )
            search_payload = pantries_service.search_products(
                query=query,
                similar=similar,
                limit=limit,
                lang=lang,
            )
            return (
                jsonify(
                    {
                        "status": "success",
                        "query": search_payload["query"],
                        "products": search_payload["products"],
                        "recommended": search_payload["recommended"],
                        # Backward compatibility with existing clients.
                        "results": search_payload["products"],
                    }
                ),
                200,
            )
        except Exception as exc:
            return _handle_error(exc)

    @pantries_bp.route("/pantry/barcode/<barcode>", methods=["GET"])
    def get_product_by_barcode(barcode: str) -> Tuple[Any, int]:
        try:
            result = pantries_service.get_open_food_facts_product(barcode)
            compatible_product = {
                # Legacy fields expected by the Android client.
                "code": result.get("openFoodFactsId", ""),
                "product_name": result.get("productName", ""),
                # Current backend fields.
                "openFoodFactsId": result.get("openFoodFactsId", ""),
                "productName": result.get("productName", ""),
                "brands": result.get("brands", ""),
                "imageUrl": result.get("imageUrl", ""),
                "nutrients": result.get("nutrients", {}),
            }
            return (
                jsonify(
                    {
                        # Keep numeric status for backward compatibility with
                        # clients that deserialize OFF-like payloads.
                        "status": 1,
                        "statusText": "success",
                        "product": compatible_product,
                    }
                ),
                200,
            )
        except Exception as exc:
            return _handle_error(exc)

    @pantries_bp.route("/pantry/add", methods=["POST"])
    def add_item() -> Tuple[Any, int]:
        payload = _get_json_payload()
        try:
            result = pantries_service.add_or_upsert_item(
                uid=payload.get("uid"),
                open_food_facts_id=payload.get("openFoodFactsId"),
                quantity_delta=payload.get("quantityDelta"),
                product_name=payload.get("productName"),
                nutrients=payload.get("nutrients"),
            )
            status_code = 201 if result.get("created") else 200
            item_payload = {
                "openFoodFactsId": result["openFoodFactsId"],
                "productName": result["productName"],
                "quantity": result["quantity"],
            }
            if result.get("nutrients"):
                item_payload["nutrients"] = result["nutrients"]
            return (jsonify({"status": "success", "item": item_payload}), status_code)
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
            if result.get("nutrients"):
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


def _handle_error(exc: Exception) -> Tuple[Any, int]:
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
