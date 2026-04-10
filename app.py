import math
import re
import unicodedata
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, request, jsonify
import firebase_admin
from firebase_admin import credentials, firestore, auth
from pantries_routes import create_pantries_blueprint
from pantries_service import PantriesService

# 1. Inizializzazione Firebase
cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

app = Flask(__name__)
app.register_blueprint(create_pantries_blueprint(db))
pantries_service = PantriesService(db)

#Non so se strettamente necessaria
def _serialize_firestore_value(value):
    """Convert Firestore values (e.g. datetime) into JSON-safe values."""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, list):
        return [_serialize_firestore_value(item) for item in value]
    if isinstance(value, dict):
        return {k: _serialize_firestore_value(v) for k, v in value.items()}
    return value


MEAL_KEYS = ("breakfast", "lunch", "dinner", "snacks")
STOPWORDS = {
    "di",
    "del",
    "della",
    "dello",
    "degli",
    "delle",
    "con",
    "al",
    "alla",
    "allo",
    "ai",
    "agli",
    "all",
    "da",
    "dal",
    "dai",
    "dalla",
    "dalle",
}


def _normalize_food_name(value: Any) -> str:
    if value is None:
        return ""
    normalized = unicodedata.normalize("NFKD", str(value))
    no_accents = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    lowered = no_accents.strip().lower()
    lowered = re.sub(r"[^a-z0-9\s]+", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def _tokenize_food_name(value: Any) -> List[str]:
    normalized = _normalize_food_name(value)
    if not normalized:
        return []
    return [
        token
        for token in normalized.split(" ")
        if token and token not in STOPWORDS and len(token) > 1
    ]


def _parse_non_negative_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(str(value).strip().replace(",", "."))
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed) or parsed < 0:
        return None
    return round(parsed, 3)


def _extract_diet_requirements(diet_payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    requirements: Dict[str, Dict[str, Any]] = {}
    days = diet_payload.get("days")
    if not isinstance(days, list):
        return requirements

    for day in days:
        if not isinstance(day, dict):
            continue
        for meal_key in MEAL_KEYS:
            items = day.get(meal_key)
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                raw_name = item.get("name")
                name = str(raw_name).strip() if raw_name is not None else ""
                if not name:
                    continue
                normalized_name = _normalize_food_name(name)
                if not normalized_name:
                    continue

                quantity = _parse_non_negative_float(item.get("quantity"))
                if quantity is None or quantity <= 0:
                    continue

                if normalized_name not in requirements:
                    requirements[normalized_name] = {
                        "name": name,
                        "requiredGrams": 0.0,
                    }
                requirements[normalized_name]["requiredGrams"] = round(
                    requirements[normalized_name]["requiredGrams"] + quantity, 3
                )
    return requirements


def _build_pantry_stock(uid: str) -> List[Dict[str, Any]]:
    pantry_items = pantries_service.list_items(uid=uid)
    stock: List[Dict[str, Any]] = []
    for item in pantry_items:
        grams = _parse_non_negative_float(item.get("grams"))
        if grams is None or grams <= 0:
            continue
        product_name = str(item.get("productName") or "").strip()
        if not product_name:
            continue
        stock.append(
            {
                "productName": product_name,
                "remainingGrams": grams,
            }
        )
    return stock


def _name_match_score(diet_name: str, pantry_name: str) -> float:
    normalized_diet = _normalize_food_name(diet_name)
    normalized_pantry = _normalize_food_name(pantry_name)
    if not normalized_diet or not normalized_pantry:
        return 0.0
    if normalized_diet == normalized_pantry:
        return 1.0

    score = 0.0
    shorter_len = min(len(normalized_diet), len(normalized_pantry))
    if shorter_len >= 4 and (
        normalized_diet in normalized_pantry or normalized_pantry in normalized_diet
    ):
        score = max(score, 0.88)

    diet_tokens = set(_tokenize_food_name(normalized_diet))
    pantry_tokens = set(_tokenize_food_name(normalized_pantry))
    if diet_tokens and pantry_tokens:
        common_tokens = diet_tokens.intersection(pantry_tokens)
        overlap = len(common_tokens) / len(diet_tokens)
        if overlap >= 1.0:
            score = max(score, 0.95)
        elif overlap >= 0.75:
            score = max(score, 0.85)
        elif overlap >= 0.5 and len(common_tokens) >= 2:
            score = max(score, 0.78)

    ratio = SequenceMatcher(None, normalized_diet, normalized_pantry).ratio()
    score = max(score, ratio)
    if score < 0.78:
        return 0.0
    return score


def _consume_matching_stock(
    pantry_stock: List[Dict[str, Any]], diet_name: str, required_grams: float
) -> float:
    candidates: List[Tuple[int, float]] = []
    for index, pantry_item in enumerate(pantry_stock):
        score = _name_match_score(diet_name, pantry_item.get("productName", ""))
        if score > 0:
            candidates.append((index, score))

    candidates.sort(key=lambda item: item[1], reverse=True)
    remaining = required_grams
    consumed = 0.0
    for candidate_index, _ in candidates:
        if remaining <= 0:
            break
        available = pantry_stock[candidate_index].get("remainingGrams", 0.0)
        if available <= 0:
            continue
        taken = min(available, remaining)
        pantry_stock[candidate_index]["remainingGrams"] = round(available - taken, 3)
        consumed = round(consumed + taken, 3)
        remaining = round(remaining - taken, 3)
    return consumed


def _format_quantity_grams(grams: float) -> str:
    rounded = round(grams, 3)
    if abs(rounded - int(rounded)) < 0.001:
        return f"{int(rounded)} g"
    return f"{rounded:g} g"


def _generate_shopping_list_items(uid: str, selected_diet_id: str) -> List[Dict[str, Any]]:
    user_ref = db.collection("users").document(uid)
    diet_doc = user_ref.collection("diets").document(selected_diet_id).get()
    if not diet_doc.exists:
        raise LookupError("Dieta non trovata")

    diet_payload = diet_doc.to_dict() or {}
    requirements = _extract_diet_requirements(diet_payload)
    pantry_stock = _build_pantry_stock(uid)

    missing_items: List[Dict[str, Any]] = []
    for requirement in requirements.values():
        food_name = requirement.get("name", "")
        required_grams = _parse_non_negative_float(requirement.get("requiredGrams")) or 0.0
        if required_grams <= 0:
            continue

        available_grams = _consume_matching_stock(
            pantry_stock=pantry_stock,
            diet_name=food_name,
            required_grams=required_grams,
        )
        missing_grams = max(required_grams - available_grams, 0.0)
        if missing_grams > 0.001:
            missing_items.append(
                {
                    "name": food_name,
                    "quantity": _format_quantity_grams(missing_grams),
                    "isChecked": False,
                }
            )

    # Rimuove duplicati mantenendo ordine.
    deduped: List[Dict[str, Any]] = []
    seen_names = set()
    for item in missing_items:
        key = _normalize_food_name(item.get("name"))
        if key in seen_names:
            continue
        seen_names.add(key)
        deduped.append(item)
    return deduped


def _shopping_list_collection(uid: str) -> Any:
    return db.collection("users").document(uid).collection("shopping_list")


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n", ""}:
            return False
    return default


def _normalize_shopping_item(raw_item: Any, index: int = 0) -> Dict[str, Any]:
    if not isinstance(raw_item, dict):
        raise ValueError(f"Elemento shoppingList non valido in posizione {index}")

    name = str(raw_item.get("name") or raw_item.get("nome") or "").strip()
    if not name:
        raise ValueError(f"name mancante nell'elemento shoppingList in posizione {index}")

    quantity_raw = raw_item.get("quantity")
    if quantity_raw is None:
        quantity_raw = raw_item.get("grammatura")
    quantity = str(quantity_raw).strip() if quantity_raw is not None else ""

    return {
        "name": name,
        "quantity": quantity,
        "isChecked": _coerce_bool(raw_item.get("isChecked"), default=False),
    }

@app.route('/get-user-data', methods=['POST'])
def get_user_data():
    """
    Endpoint chiamato dall'app Android dopo il login.
    Riceve il Token o l'UID e restituisce il profilo completo da Firestore.
    """
    data = request.json
    uid = data.get('uid')
    email = data.get('email')

    if not uid:
        return jsonify({"error": "UID mancante"}), 400

    try:
        # Riferimento al documento dell'utente usando l'UID come ID
        user_ref = db.collection('users').document(uid)
        doc = user_ref.get()

        if doc.exists:
            # L'utente esiste, restituiamo i dati (peso, altezza, kcal, ecc.)
            return jsonify({
                "status": "existing_user",
                "userData": doc.to_dict()
            }), 200
        else:
            # L'utente è nuovo (primo login), creiamo il documento base
            new_user_data = {
                "uid": uid,
                "email": email,
                "name": data.get('name', 'Nuovo Utente'),
                "biometrics": {
                    "age": 0, "height": 0, "weight": 0, "gender": ""
                },
                "goals": {
                    "dailyKcal": 0,
                    "fitnessGoal": "maintainance",
                    "macrosTarget": {"carbs": 0, "protein": 0, "fat": 0}
                },
                "profile_image_url": "",
                "manualOverride": False
            }
            user_ref.set(new_user_data)
            return jsonify({
                "status": "new_user",
                "userData": new_user_data
            }), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/register-user', methods=['POST'])
def register_user():
    """
    Crea un nuovo utente sia in Firebase Authentication sia in Firestore.

    Passaggi:
    1. Usa `auth.create_user` per registrare email e password in Auth.
    2. Salva un documento nella collezione `users` con l'UID restituito.

    Corpo JSON atteso:
    {
        "email": "user@example.com",       // obbligatorio
        "password": "secret123",          // obbligatorio
        // campi opzionali da inserire direttamente nel documento:
        "name": "Nome Utente",
        "biometrics": {"age":0, "height":0, "weight":0, "gender":""},
        "goals": {"dailyKcal":0, "fitnessGoal":"maintainance","macrosTarget":{"carbs":0,"protein":0,"fat":0}},
        "profile_image_url": "",
        "manualOverride": false
    }

    Valida presenza di email/password e restituisce 400 se mancanti.
    Se la creazione Auth fallisce (es. email già usata) restituisce 400/500.
    """
    data = request.json or {}
    email = data.get('email')
    password = data.get('password')

    if not email or not password:
        return jsonify({"error": "email e password obbligatorie"}), 400

    try:
        # crea utente in Firebase Auth
        user_record = auth.create_user(email=email, password=password)
        uid = user_record.uid

        # prepare dati base
        biometrics = data.get('biometrics') or {"age":0, "height":0, "weight":0, "gender":""}
        goals_in = data.get('goals', {})
        manual_override = bool(data.get('manualOverride', False))

        # se sono presenti biometrics e fitnessGoal, calcoliamo i valori come in update_user
        if biometrics and goals_in.get('fitnessGoal') and not manual_override:
            try:
                weight = float(biometrics['weight'])
                height = float(biometrics['height'])
                age = int(biometrics['age'])
                gender = biometrics['gender'].lower()
                activity_multiplier = float(biometrics.get('activityLevel', 1.2))
                fitness_goal = goals_in.get('fitnessGoal', 'maintainance').lower()

                # BMR
                if gender == 'm' or gender == 'male':
                    bmr = (10 * weight) + (6.25 * height) - (5 * age) + 5
                else:
                    bmr = (10 * weight) + (6.25 * height) - (5 * age) - 161

                tdee_base = int(bmr * activity_multiplier)
                if fitness_goal == 'deficit':
                    daily_kcal = tdee_base - 500
                elif fitness_goal == 'surplus':
                    daily_kcal = tdee_base + 300
                else:
                    daily_kcal = tdee_base

                macros = {
                    "carbs": int((daily_kcal * 0.50) / 4),
                    "protein": int((daily_kcal * 0.20) / 4),
                    "fat": int((daily_kcal * 0.30) / 9)
                }

                goals = {
                    "dailyKcal": daily_kcal,
                    "fitnessGoal": fitness_goal,
                    "macrosTarget": macros
                }
            except Exception:
                # se qualcosa va storto nel calcolo, ricadiamo su valori di default
                goals = goals_in or {"dailyKcal":0, "fitnessGoal":"maintainance", "macrosTarget":{"carbs":0,"protein":0,"fat":0}}
        else:
            goals = goals_in or {"dailyKcal":0, "fitnessGoal":"maintainance", "macrosTarget":{"carbs":0,"protein":0,"fat":0}}

        doc = {
            "uid": uid,
            "email": email,
            "name": data.get('name', 'Nuovo Utente'),
            "biometrics": biometrics,
            "goals": goals,
            "profile_image_url": data.get('profile_image_url', ''),
            "manualOverride": manual_override
        }
        db.collection('users').document(uid).set(doc)
        return jsonify({"status": "created", "uid": uid, "userData": doc}), 201
    except Exception as e:
        # propagate error message (es. email già in uso)
        return jsonify({"error": str(e)}), 500

@app.route('/update-user', methods=['POST'])
def update_user():
    data = request.json
    uid = data.get('uid')
    name = data.get('name') # <-- Recuperiamo il nome
    # Estraiamo i dati biometrici inviati da Android
    bio = data.get('biometrics') # {weight, height, age, gender, activityLevel}
    goals = data.get('goals') # {dailyKcal, macrosTarget, fitnessGoal}
    # Nuovo campo: url immagine profilo
    profile_image_url = data.get('profile_image_url')
    # Se true, user fornisce manualmente dailyKcal e macrosTarget
    manual_override = bool(data.get('manualOverride', False))
    
    if not uid or not bio:
        return jsonify({"error": "Dati incompleti"}), 400

    try:
        # Se l'utente richiede override manuale, usiamo i valori forniti
        if manual_override:
            if not goals:
                return jsonify({"error": "manualOverride true ma 'goals' mancante"}), 400
            try:
                daily_kcal = int(goals.get('dailyKcal'))
                macros = goals.get('macrosTarget') or {}
                # Assicuriamo la presenza dei tre macro in forma numerica
                macros = {
                    'carbs': int(macros.get('carbs', 0)),
                    'protein': int(macros.get('protein', 0)),
                    'fat': int(macros.get('fat', 0))
                }
                fitness_goal = goals.get('fitnessGoal', 'maintainance').lower()
            except Exception as ex:
                return jsonify({"error": f"Goals invalidi: {str(ex)}"}), 400
        else:
            # --- LOGICA DI CALCOLO TDEE (Total Daily Energy Expenditure) ---
            weight = float(bio['weight'])
            height = float(bio['height'])
            age = int(bio['age'])
            gender = bio['gender'].lower()
            activity_multiplier = float(bio.get('activityLevel', 1.2))
            fitness_goal = (goals.get('fitnessGoal') if goals else 'maintainance').lower() # 'lose', 'gain', 'maintain'

            # 1. Calcolo BMR (Metabolismo Basale)
            if gender == 'm' or gender == 'male':
                bmr = (10 * weight) + (6.25 * height) - (5 * age) + 5
            else:
                bmr = (10 * weight) + (6.25 * height) - (5 * age) - 161

            # 2. Calcolo TDEE (Calorie totali basate sull'attività)
            tdee_base = int(bmr * activity_multiplier)

            if fitness_goal == 'deficit':
                daily_kcal = tdee_base - 500  # Deficit standard di 500 kcal
            elif fitness_goal == 'surplus':
                daily_kcal = tdee_base + 300  # Surplus moderato di 300 kcal
            else:
                daily_kcal = tdee_base        # Mantenimento

            # 3. Ripartizione Macros Standard (Esempio: 50% Carbs, 20% Prot, 30% Grassi)
            # 1g carb/prot = 4kcal, 1g grassi = 9kcal
            macros = {
                "carbs": int((daily_kcal * 0.50) / 4),
                "protein": int((daily_kcal * 0.20) / 4),
                "fat": int((daily_kcal * 0.30) / 9)
            }

        # --- AGGIORNAMENTO FIRESTORE ---
        user_ref = db.collection('users').document(uid)
        update_data = {
            "name": name,
            "biometrics": bio,
            "goals": {
                "dailyKcal": daily_kcal,
                "macrosTarget": macros,
                "fitnessGoal": fitness_goal
            }
        }
        # Salviamo lo stato di manualOverride nel documento utente
        update_data['manualOverride'] = manual_override
        # Se fornita, aggiungiamo l'URL dell'immagine profilo
        if profile_image_url:
            update_data['profile_image_url'] = profile_image_url
        
        user_ref.update(update_data)

        return jsonify({
            "status": "success",
            "dailyKcal": daily_kcal,
            "macros": macros
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/save-diet', methods=['POST'])
def save_diet():
    data = request.json or {}
    uid = data.get('uid')
    diet_data = data.get('dietData')

    if uid is None:
        return jsonify({"error": "UID mancante"}), 400

    if diet_data is None:
        return jsonify({"error": "dietData mancante"}), 401


    try:
        user_ref = db.collection('users').document(uid)
        user_doc = user_ref.get()
        if not user_doc.exists:
            return jsonify({"error": "Utente non trovato"}), 404

        diets_ref = user_ref.collection("diets")

        selected_diet_id = diet_data.get("selectedDietId")
        diets_to_save = diet_data.get("diets") or []

        saved_ids = []

        if diets_to_save:
            batch = db.batch()
            for index, entry in enumerate(diets_to_save):
                if not isinstance(entry, dict):
                    return jsonify({
                        "error": f"Elemento dieta non valido in posizione {index}: atteso oggetto"
                    }), 400

                diet_id = entry.get("duid", None)
                if not diet_id:
                    return jsonify({
                        "error": f"DUID mancante nell'elemento dieta in posizione {index}"
                    }), 400

                diet_doc = dict(entry)
                #Adding timestamp
                diet_doc["createdAt"] = firestore.SERVER_TIMESTAMP
                # Canonical key in Firestore: keep only lowercase "duid".
                diet_doc["duid"] = diet_id

                doc_ref = diets_ref.document(diet_id)
                batch.set(doc_ref, diet_doc, merge=True)
                saved_ids.append(diet_id)

            batch.commit()

        if selected_diet_id is not None:
            user_ref.set({"selectedDietId": selected_diet_id}, merge=True)

        return jsonify({
            "status": "success",
            "savedDietIds": saved_ids,
            "selectedDietId": selected_diet_id
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/get-diet', methods=['POST'])
def get_diet():
    data = request.json or {}
    uid = data.get('uid')

    if not uid:
        return jsonify({"error": "UID mancante"}), 400

    try:
        user_ref = db.collection('users').document(uid)
        user_doc = user_ref.get()

        if not user_doc.exists:
            return jsonify({"error": "Utente non trovato"}), 404

        user_data = user_doc.to_dict() or {}
        selected_diet_id = user_data.get("selectedDietId")

        diet_docs = user_ref.collection("diets").stream()
        diets = []
        for diet_doc in diet_docs:
            diet_item = diet_doc.to_dict() or {}
            diet_id = diet_item.get("duid", None)
            diet_item["duid"] = diet_id
            diets.append(_serialize_firestore_value(diet_item))

        diet_payload = {
            "diets": diets,
            "selectedDietId": selected_diet_id
        }
        return jsonify({
            "status": "success",
            "diets": diets,
            "dietData": diet_payload
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/delete-diet', methods=['POST'])
def delete_diet():
    data = request.json or {}
    uid = data.get('uid')
    diet_id = data.get('duid') or data.get('diud')

    if not uid:
        return jsonify({"error": "UID mancante"}), 400

    if not diet_id:
        return jsonify({"error": "DUID mancante"}), 400

    try:
        user_ref = db.collection('users').document(uid)
        user_doc = user_ref.get()

        if not user_doc.exists:
            return jsonify({"error": "Utente non trovato"}), 404

        diet_ref = user_ref.collection("diets").document(diet_id)
        diet_doc = diet_ref.get()

        if not diet_doc.exists:
            return jsonify({"error": "Dieta non trovata"}), 404

        user_data = user_doc.to_dict() or {}
        selected_diet_id = user_data.get("selectedDietId")

        batch = db.batch()
        batch.delete(diet_ref)

        if selected_diet_id == diet_id:
            selected_diet_id = None
            batch.set(user_ref, {"selectedDietId": None}, merge=True)

        batch.commit()

        return jsonify({
            "status": "success",
            "deletedDietId": diet_id,
            "selectedDietId": selected_diet_id
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/generate_shopping_list', methods=['POST'])
def generate_shopping_list():
    data = request.json or {}
    uid = str(data.get("uid") or "").strip()
    selected_diet_id = str(data.get("selectedDietId") or "").strip()

    if not uid:
        return jsonify({"error": "UID mancante"}), 400
    if not selected_diet_id:
        return jsonify({"error": "selectedDietId mancante"}), 400

    try:
        user_doc = db.collection("users").document(uid).get()
        if not user_doc.exists:
            return jsonify({"error": "Utente non trovato"}), 404

        shopping_list = _generate_shopping_list_items(
            uid=uid,
            selected_diet_id=selected_diet_id,
        )

        return jsonify(shopping_list), 200
    except LookupError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/get_shopping_list', methods=['POST'])
def get_shopping_list():
    data = request.json or {}
    uid = str(data.get("uid") or "").strip()

    if not uid:
        return jsonify({"error": "UID mancante"}), 400

    try:
        user_doc = db.collection("users").document(uid).get()
        if not user_doc.exists:
            return jsonify({"error": "Utente non trovato"}), 404

        docs = _shopping_list_collection(uid).stream()
        shopping_list: List[Dict[str, Any]] = []
        for doc in docs:
            payload = doc.to_dict() or {}
            item_name = str(payload.get("name") or payload.get("nome") or doc.id).strip()
            if not item_name:
                continue

            quantity_raw = payload.get("quantity")
            if quantity_raw is None:
                quantity_raw = payload.get("grammatura")
            item_quantity = str(quantity_raw).strip() if quantity_raw is not None else ""

            shopping_list.append(
                {
                    "name": item_name,
                    "quantity": item_quantity,
                    "isChecked": _coerce_bool(payload.get("isChecked"), default=False),
                }
            )

        shopping_list.sort(key=lambda x: x["name"].lower())
        return jsonify({"status": "success", "shoppingList": shopping_list}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/update_shopping_list', methods=['POST'])
def update_shopping_list():
    data = request.json or {}
    uid = str(data.get("uid") or "").strip()

    if not uid:
        return jsonify({"error": "UID mancante"}), 400

    raw_items = data.get("shoppingList")
    if raw_items is None and isinstance(data.get("item"), dict):
        raw_items = [data.get("item")]
    if not isinstance(raw_items, list):
        return jsonify({"error": "shoppingList deve essere un array non vuoto"}), 400

    replace = _coerce_bool(data.get("replace"), default=False)

    normalized_items: List[Dict[str, Any]] = []
    normalized_by_name: Dict[str, Dict[str, Any]] = {}
    try:
        for index, raw_item in enumerate(raw_items):
            item = _normalize_shopping_item(raw_item, index=index)
            key = _normalize_food_name(item["name"])
            normalized_by_name[key] = item
        normalized_items = list(normalized_by_name.values())
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    try:
        user_ref = db.collection("users").document(uid)
        user_doc = user_ref.get()
        if not user_doc.exists:
            return jsonify({"error": "Utente non trovato"}), 404

        shopping_ref = _shopping_list_collection(uid)
        batch = db.batch()

        if replace:
            existing_docs = list(shopping_ref.stream())
            allowed_doc_ids = {item["name"] for item in normalized_items}
            for doc in existing_docs:
                if doc.id not in allowed_doc_ids:
                    batch.delete(shopping_ref.document(doc.id))

        for item in normalized_items:
            doc_ref = shopping_ref.document(item["name"])
            batch.set(
                doc_ref,
                {
                    "name": item["name"],
                    "grammatura": item["quantity"],
                    "isChecked": item["isChecked"],
                    "updatedAt": firestore.SERVER_TIMESTAMP,
                },
                merge=True,
            )

        batch.commit()

        return jsonify(
            {
                "status": "success",
                "updatedCount": len(normalized_items),
                "replace": replace,
            }
        ), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
