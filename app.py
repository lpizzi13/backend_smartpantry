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
                    "dailyKcal": 0
                },
                "firstLogin": True
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
    
    if not uid or not bio:
        return jsonify({"error": "Dati incompleti"}), 400

    try:
        # --- LOGICA DI CALCOLO TDEE (Total Daily Energy Expenditure) ---
        weight = float(bio['weight'])
        height = float(bio['height'])
        age = int(bio['age'])
        gender = bio['gender'].lower()
        activity_multiplier = float(bio.get('activityLevel', 1.2))

        # 1. Calcolo BMR (Metabolismo Basale)
        if gender == 'm' or gender == 'male':
            bmr = (10 * weight) + (6.25 * height) - (5 * age) + 5
        else:
            bmr = (10 * weight) + (6.25 * height) - (5 * age) - 161

        # 2. Calcolo TDEE (Calorie totali basate sull'attività)
        daily_kcal = int(bmr * activity_multiplier)

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
                "macrosTarget": macros
            },
            "firstLogin": False
        }
        
        user_ref.update(update_data)

        return jsonify({
            "status": "success",
            "dailyKcal": daily_kcal,
            "macros": macros
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)