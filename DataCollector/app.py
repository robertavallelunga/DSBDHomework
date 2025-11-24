import os
import grpc
from flask import Flask, request, jsonify
from database import init_db, get_db_connection
import user_pb2
import user_pb2_grpc

app = Flask(__name__)

GRPC_HOST=os.getenv("TARGET_GRPC_HOST", "userManager")
GRPC_PORT=os.getenv("TARGET_GRPC_PORT", 50051)

def check_user_exists_grpc(email):
    """Chiama User Manager via gRPC per verificare l'utente"""
    channel = grpc.insecure_channel(f'{GRPC_HOST}:{GRPC_PORT}')
    stub = user_pb2_grpc.UserServiceStub(channel)
    try:
        response = stub.UserExists(user_pb2.UserRequest(email=email))
        return response.exists
    except grpc.RpcError as e:
        print(f"gRPC Error: {e}")
        return False

@app.route('/add', methods=['POST'])
def add_interest():
    """Aggiunge interesse per un aeroporto se l'utente esiste"""
    data = request.json
    email = data.get('email')
    airport = data.get('airport')
#Controllo valori passati alla richiesta
    if not email or not airport:
        return jsonify({
            "error": "Dati mancanti o formato non valido: 'email' (string) e 'airports' sono richiesti."
        }), 400

    # 1. Verifica gRPC
    if not check_user_exists_grpc(email):
        return jsonify({"error": "User non trovato nel database"}), 404

    # 2. Salva interesse
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO interests (user_email, cod_aeroporto) VALUES (%s, %s)", (email, airport))
        conn.commit()
        return jsonify({"message": "Aeroporto aggiunto"}), 201
    except Exception as e:
        conn.rollback()
        return jsonify({"error": "L'aeroporto è già inserito negli interessi"}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/view', methods=['GET'])
def visualizza_interessi():
    connection = get_db_connection()
    cursor = connection.cursor()
    user_email = request.args.get('email')
    # 1. Verifica gRPC
    if not check_user_exists_grpc(user_email):
        return jsonify({"error": "User non trovato nel database"}), 404
    try:
        select_query = "SELECT id,user_email,cod_aeroporto FROM interests WHERE email_user=%s"
        cursor.execute(select_query, (user_email,))
        risultati = cursor.fetchall()
        interessi_list = []
        for row in risultati:
            interessi_list.append({
                "id": row[0],
                "email_user": row[1],
                "cod_aeroporto": row[2]
            })
        return jsonify(interessi_list), 200
    except Exception as e:
        # Gestione degli errori di database
        print(f"Errore durante l'esecuzione della query: {e}")
        return jsonify({
            "error": "Errore interno del server durante il recupero degli interessi."
        }), 500
    finally:
        cursor.close()
        connection.close()

@app.route('/delete_interest', methods=['DELETE'])
def cancella_interessi():
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        data = request.json
        user_email = data.get('email')
        cod_aeroporto = data.get('airport')
    # 1. Verifica gRPC
        if not check_user_exists_grpc(user_email):
          return jsonify({"error": "User non trovato nel database"}), 404

        select_query = "DELETE FROM interests WHERE email_user=%s AND cod_aeroporto=%s"
        cursor.execute(select_query, (user_email,cod_aeroporto,))
        connection.commit()
        return jsonify({
            "message": "Interesse eliminato con successo.",
            "email": user_email,
            "airport": cod_aeroporto
        }), 200
    except Exception as e:
        print(f"Errore durante l'esecuzione della query: {e}")
        return jsonify({
            "error": "Errore interno del server durante il recupero degli interessi."
        }), 500
    finally:
        cursor.close()
        connection.close()

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000)
