from typing import Any, Dict, Tuple

from flask import Blueprint, jsonify, request

from home_service import HomeError, HomeService


def create_home_blueprint(db: Any) -> Blueprint:
    home_bp = Blueprint("home", __name__)
    home_service = HomeService(db)

    @home_bp.route("/home/add", methods=["POST"])
    def add_home_entry() -> Tuple[Any, int]:
        payload = _get_json_payload()
        try:
            result = home_service.add_entry(
                uid=payload.get("uid"),
                date_key=payload.get("dateKey"),
                open_food_facts_id=payload.get("openFoodFactsId"),
                meal_type=payload.get("mealType"),
                source=payload.get("source"),
                product_name=payload.get("productName"),
                grams=payload.get("grams"),
                nutrients=payload.get("nutrients"),
            )
            return jsonify(
                {
                    "status": "ok",
                    "dateKey": result["dateKey"],
                    "totals": result["totals"],
                    "entriesCount": result["entriesCount"],
                }
            ), 200
        except Exception as exc:
            return _handle_error(exc)

    @home_bp.route("/home/<uid>/<date_key>", methods=["GET"])
    def get_home_day(uid: str, date_key: str) -> Tuple[Any, int]:
        try:
            result = home_service.get_day(uid=uid, date_key=date_key)
            return jsonify(
                {
                    "status": "ok",
                    "dateKey": result["dateKey"],
                    "totals": result["totals"],
                    "entriesCount": result["entriesCount"],
                    "meals": result["meals"],
                }
            ), 200
        except Exception as exc:
            return _handle_error(exc)

    @home_bp.route("/home/update", methods=["PATCH"])
    def patch_home_entry() -> Tuple[Any, int]:
        payload = _get_json_payload()
        try:
            result = home_service.patch_entry(
                uid=payload.get("uid"),
                date_key=payload.get("dateKey"),
                open_food_facts_id=payload.get("openFoodFactsId"),
                meal_type=payload.get("mealType"),
                grams=payload.get("grams"),
                nutrients=payload.get("nutrients"),
            )
            return jsonify(
                {
                    "status": "ok",
                    "dateKey": result["dateKey"],
                    "totals": result["totals"],
                    "entriesCount": result["entriesCount"],
                }
            ), 200
        except Exception as exc:
            return _handle_error(exc)

    @home_bp.route("/home/delete", methods=["DELETE"])
    def delete_home_entry() -> Tuple[Any, int]:
        payload = _get_json_payload()
        try:
            result = home_service.delete_entry(
                uid=payload.get("uid"),
                date_key=payload.get("dateKey"),
                open_food_facts_id=payload.get("openFoodFactsId"),
            )
            return jsonify(
                {
                    "status": "ok",
                    "dateKey": result["dateKey"],
                    "totals": result["totals"],
                    "entriesCount": result["entriesCount"],
                }
            ), 200
        except Exception as exc:
            return _handle_error(exc)

    return home_bp


def _get_json_payload() -> Dict[str, Any]:
    payload = request.get_json(silent=True)
    if isinstance(payload, dict):
        return payload
    return {}


def _handle_error(exc: Exception) -> Tuple[Any, int]:
    if isinstance(exc, HomeError):
        return jsonify({"status": "error", "error": exc.message}), exc.status_code
    return jsonify({"status": "error", "error": "Errore interno del server"}), 500
