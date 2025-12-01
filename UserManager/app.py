import threading
from flask import Flask, request, jsonify
from database import get_db_connection, init_db
from grpc_server import serve_grpc
app = Flask(__name__)

@app.route('/new', methods=['POST'])
def register_user():
    data = request.json
    required_fields = ['email', 'nome', 'cognome', 'messageID']

    # Controllo dei parametri mancanti
    missing_fields = [field for field in required_fields if not data or field not in data]

    if missing_fields:
        return jsonify({
            "error": "Parametri mancanti nella richiesta",
            "missing_fields": missing_fields
        }), 400

    email = data.get('email')
    messageID=data.get('messageID')
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM requestID WHERE messageID = %s", (messageID, ))
        if cursor.fetchone():
            response="Richiesta elaborata con successo"
            return jsonify({"message": f"Richiesta già elaborata, esito richiesta precendente: {response}"}), 200

        cursor.execute("SELECT * FROM users WHERE email = %s", (email, ))
        if cursor.fetchone():
            response="Utente già registrato"
            cursor.execute("INSERT INTO requestID (messageID, response) VALUES (%s, %s)", (messageID,response))
            conn.commit()
            return jsonify({"message": "User esistente"}), 200

        response="Richiesta elaborata con successo"
        cursor.execute("INSERT INTO requestID (messageID, response) VALUES (%s, %s)", (messageID,response))
        cursor.execute("INSERT INTO users (email, nome, cognome) VALUES (%s, %s, %s)", (email, data.get('nome'), data.get('cognome')))
        conn.commit()
        return jsonify({"message": "User registrato"}), 201
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/delete', methods=['DELETE'])
def remove_user():
    data = request.json
    email = data.get('email')
    # Controllo essenziale: Assicuro che l'email sia presente
    if not email:
        return jsonify({"error": "Email mancante nella richiesta"}), 400
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("DELETE FROM users WHERE email = %s", (email,))
        # L'attributo rowcount restituisce il numero di righe modificate dall'ultima query, controllo quante ne sono state cancellate
        righe_cancellate = cursor.rowcount
        connection.commit()
        if righe_cancellate > 0:
            return jsonify({"message": f"User con email '{email}' cancellato con successo"}), 200
        else:
            # Se rowcount è 0, significa che l'utente non è stato trovato
            return jsonify({"message": f"Nessun user trovato con l'email '{email}'"}), 404
    except Exception as e:
        connection.rollback()
        return jsonify({"error": f"Errore durante la cancellazione: {str(e)}"}), 500
    finally:
        cursor.close()
        connection.close()

if __name__ == '__main__':
    init_db()

    grpc_thread = threading.Thread(target=serve_grpc)
    grpc_thread.daemon = True
    grpc_thread.start()

    app.run(host='0.0.0.0', port=5000)
