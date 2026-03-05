from flask import Flask, request, jsonify
import firebase_admin
from firebase_admin import credentials, firestore, auth

# 1. Inizializzazione Firebase
cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

app = Flask(__name__)

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
        user_ref.set({"dietData": diet_data}, merge=True)
        return jsonify({"status": "success"}), 200
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
        doc = user_ref.get()

        if not doc.exists:
            return jsonify({"error": "Utente non trovato"}), 404

        user_data = doc.to_dict() or {}
        return jsonify({
            "status": "success",
            "dietData": user_data.get("dietData")
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
