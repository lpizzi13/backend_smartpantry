from flask import Flask, request, jsonify
import firebase_admin
from firebase_admin import credentials, firestore, auth

# 1. Inizializzazione Firebase
cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

app = Flask(__name__)


def _as_diet_document(entry):
    """Normalize any diet entry into a Firestore-storable document."""
    if isinstance(entry, dict):
        diet_doc = dict(entry)
    else:
        diet_doc = {"value": entry}

    # Add server timestamp if caller did not provide one.
    if "createdAt" not in diet_doc:
        diet_doc["createdAt"] = firestore.SERVER_TIMESTAMP
    return diet_doc


def _extract_diet_id(entry):
    """Extract DUID from diet payload using known key variants."""
    if not isinstance(entry, dict):
        return None

    for key in ("DUID", "duid", "dietId", "diet_id", "id"):
        value = entry.get(key)
        if value is None:
            continue
        value_str = str(value).strip()
        if value_str:
            return value_str
    return None


def _parse_diet_payload(diet_data):
    """Support both legacy dietData and Android envelope {diets, selectedDietId}."""
    selected_diet_id = None

    if isinstance(diet_data, dict) and "diets" in diet_data:
        selected_diet_id = diet_data.get("selectedDietId")
        diets_container = diet_data.get("diets") or []
    else:
        diets_container = diet_data

    if isinstance(diets_container, list):
        diets_to_save = diets_container
    elif isinstance(diets_container, dict):
        diets_to_save = [diets_container]
    else:
        return None, None, "dietData non valido: attesi oggetti dieta o lista di oggetti"

    if selected_diet_id is not None:
        selected_diet_id = str(selected_diet_id).strip() or None

    return diets_to_save, selected_diet_id, None


def _serialize_firestore_value(value):
    """Convert Firestore values (e.g. datetime) into JSON-safe values."""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, list):
        return [_serialize_firestore_value(item) for item in value]
    if isinstance(value, dict):
        return {k: _serialize_firestore_value(v) for k, v in value.items()}
    return value

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

    if not uid:
        return jsonify({"error": "UID mancante"}), 400

    if diet_data is None:
        return jsonify({"error": "dietData mancante"}), 400


    try:
        user_ref = db.collection('users').document(uid)
        user_doc = user_ref.get()
        if not user_doc.exists:
            return jsonify({"error": "Utente non trovato"}), 404

        diets_ref = user_ref.collection("diets")
        diets_to_save, selected_diet_id, parse_error = _parse_diet_payload(diet_data)
        if parse_error:
            return jsonify({"error": parse_error}), 400

        saved_ids = []

        if diets_to_save:
            batch = db.batch()
            for index, entry in enumerate(diets_to_save):
                if not isinstance(entry, dict):
                    return jsonify({
                        "error": f"Elemento dieta non valido in posizione {index}: atteso oggetto"
                    }), 400

                diet_id = _extract_diet_id(entry)
                if not diet_id:
                    return jsonify({
                        "error": f"DUID mancante nell'elemento dieta in posizione {index}"
                    }), 400

                diet_doc = _as_diet_document(entry)
                # Canonical key in Firestore: keep only lowercase "duid".
                diet_doc["duid"] = diet_id
                # Remove legacy aliases if present in existing docs.
                diet_doc["DUID"] = firestore.DELETE_FIELD
                diet_doc["dietId"] = firestore.DELETE_FIELD
                diet_doc["diet_id"] = firestore.DELETE_FIELD
                diet_doc["id"] = firestore.DELETE_FIELD

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
            diet_id = _extract_diet_id(diet_item) or diet_doc.id
            diet_item["duid"] = diet_id
            diet_item.pop("DUID", None)
            diet_item.pop("dietId", None)
            diet_item.pop("diet_id", None)
            diet_item.pop("id", None)
            diets.append(_serialize_firestore_value(diet_item))

        if diets:
            diet_payload = {
                "diets": diets,
                "selectedDietId": selected_diet_id
            }
            return jsonify({
                "status": "success",
                "diets": diets,
                "dietData": diet_payload
            }), 200

        # Fallback legacy: previously saved as users/{uid}.dietData
        legacy_diets = user_data.get("dietData")
        if legacy_diets is None:
            legacy_diets = []
        elif not isinstance(legacy_diets, list):
            legacy_diets = [legacy_diets]

        serialized_legacy_diets = _serialize_firestore_value(legacy_diets)
        diet_payload = {
            "diets": serialized_legacy_diets,
            "selectedDietId": selected_diet_id
        }

        return jsonify({
            "status": "success",
            "diets": serialized_legacy_diets,
            "dietData": diet_payload
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
