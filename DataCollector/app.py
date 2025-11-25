import datetime
import os
import grpc
from flask import Flask, request, jsonify
from database import init_db, get_db_connection
import user_pb2
import user_pb2_grpc
from opensky import token, voli_arrivo

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
        cursor.execute("INSERT INTO interests (email_user, cod_aeroporto) VALUES (%s, %s)", (email, airport))
        conn.commit()
        return jsonify({"message": "Aeroporto aggiunto"}), 201
    except Exception as e:
        conn.rollback()
        return jsonify({"error": "L'aeroporto è già inserito negli interessi"}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/view_interests', methods=['GET'])
def visualizza_interessi():
    connection = get_db_connection()
    cursor = connection.cursor()
    user_email = request.args.get('email')
    # 1. Verifica gRPC
    if not check_user_exists_grpc(user_email):
        return jsonify({"error": "User non trovato nel database"}), 404
    try:
        select_query = "SELECT id,email_user,cod_aeroporto FROM interests WHERE email_user=%s"
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

@app.route('/get_data', methods=['GET'])
def get_data():
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        select_query = "SELECT * FROM interests"
        cursor.execute(select_query)
        risultati = cursor.fetchall()
        interests_list = []
        for row in risultati:
            interests_list.append({
                "id": row[0],
                "email_user": row[1],
                "cod_aeroporto": row[2]
            })

        for user in interests_list:
            if not check_user_exists_grpc(user['email_user']):
                cursor.execute("DELETE FROM interests WHERE email_user=%s", (user['email_user'], ))
                connection.commit()
                print("Interessi vecchi eliminati!")

        select_query = "SELECT DISTINCT cod_aeroporto FROM interests"
        cursor.execute(select_query)
        risultati = cursor.fetchall()

        if not risultati:
            return jsonify({"message": "Nessun interesse trovato. Nessun dato scaricato.", "count": 0}), 200

        token()

        total_flights_count = 0

        for row in risultati:
            cod_aeroporto = row[0]
            data = voli_arrivo(cod_aeroporto)

            if not data:
                print(f"Nessun volo trovato per {cod_aeroporto}")
                continue

            total_flights_count += len(data)
            print(f"Inserimento dati per {cod_aeroporto}...")

            for volo in data:
                icao_partenza = volo.get('estDepartureAirport')
                icao_arrivo = volo.get('estArrivalAirport')

                ts_start = volo.get('firstSeen')
                ts_end = volo.get('lastSeen')

                ora_partenza = datetime.datetime.fromtimestamp(ts_start) if ts_start else None
                ora_arrivo = datetime.datetime.fromtimestamp(ts_end) if ts_end else None

                valori = (icao_partenza, icao_arrivo, ora_partenza, ora_arrivo)
                cursor.execute("""
                               INSERT INTO flights
                                   (icao_partenza, icao_arrivo, orario_partenza, orario_arrivo)
                               VALUES (%s, %s, %s, %s)
                               """, valori)
            connection.commit()
        return jsonify({"message": "Dati recuperati e salvati con successo", "count": total_flights_count}), 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Errore durante l'esecuzione della query: {e}")
        return jsonify({"error": f"Errore interno del server: {str(e)}"}), 500
    finally:
        cursor.close()
        connection.close()

@app.route('/view_flights', methods=['GET'])
def visualizza_voli():
    connection = get_db_connection()
    cursor = connection.cursor()
    aeroporto = request.args.get('cod_aeroporto')

    try:
        select_query = "SELECT * FROM flights WHERE icao_partenza=%s OR icao_arrivo=%s"
        cursor.execute(select_query, (aeroporto,aeroporto))
        risultati = cursor.fetchall()
        flights_list = []
        for row in risultati:
            flights_list.append({
                "id": row[0],
                "icao_partenza": row[1],
                "icao_arrivo": row[2],
                "orario_partenza" : row[3],
                "orario_arrivo" : row[4]
            })
        return jsonify(flights_list), 200
    except Exception as e:
        # Gestione degli errori di database
        print(f"Errore durante l'esecuzione della query: {e}")
        return jsonify({
            "error": "Errore interno del server durante il recupero dei voli."
        }), 500
    finally:
        cursor.close()
        connection.close()


if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000)
