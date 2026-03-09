from flask import Flask, request, jsonify
import firebase_admin
from firebase_admin import credentials, firestore, auth
from pantries_routes import create_pantries_blueprint

# 1. Inizializzazione Firebase
cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

app = Flask(__name__)
app.register_blueprint(create_pantries_blueprint(db))

@app.route('/user/login', methods=['POST'])
def login():
    """Alias per /get-user-data per compatibilità con il frontend"""
    return get_user_data()

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
@app.route('/user/update', methods=['POST'])
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
