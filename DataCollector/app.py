import datetime
import os
import grpc
import time
import threading
from flask import Flask, request, jsonify
from database import init_db, get_db_connection
import user_pb2
import user_pb2_grpc
from opensky import token, voli_arrivo

from apscheduler.schedulers.background import BackgroundScheduler
import logging

# Configurazione del logging, si avvia ogni volta che si attiva lo scheduler
logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.INFO)

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
                icao_24= volo.get('icao24')
                icao_partenza = volo.get('estDepartureAirport')
                icao_arrivo = volo.get('estArrivalAirport')

                ts_start = volo.get('firstSeen')
                ts_end = volo.get('lastSeen')

                ora_partenza = datetime.datetime.fromtimestamp(ts_start) if ts_start else None
                ora_arrivo = datetime.datetime.fromtimestamp(ts_end) if ts_end else None

                valori = (icao_24,icao_partenza, icao_arrivo, ora_partenza, ora_arrivo)
                cursor.execute("""
                               INSERT INTO flights
                                   (icao_24,icao_partenza, icao_arrivo, orario_partenza, orario_arrivo)
                               VALUES (%s,%s, %s, %s, %s)
                                   ON DUPLICATE KEY UPDATE
                                                        icao_arrivo = VALUES(icao_arrivo),
                                                        orario_arrivo = VALUES(orario_arrivo)
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

def get_data_scheduler():
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
                icao_24=volo.get('icao24')
                icao_partenza = volo.get('estDepartureAirport')
                icao_arrivo = volo.get('estArrivalAirport')

                ts_start = volo.get('firstSeen')
                ts_end = volo.get('lastSeen')

                ora_partenza = datetime.datetime.fromtimestamp(ts_start) if ts_start else None
                ora_arrivo = datetime.datetime.fromtimestamp(ts_end) if ts_end else None

                valori = (icao_24,icao_partenza, icao_arrivo, ora_partenza, ora_arrivo)
                cursor.execute("""
                               INSERT INTO flights
                                   (icao_24,icao_partenza, icao_arrivo, orario_partenza, orario_arrivo)
                               VALUES (%s,%s, %s, %s, %s)
                                   ON DUPLICATE KEY UPDATE
                                                        icao_arrivo = VALUES(icao_arrivo),
                                                        orario_arrivo = VALUES(orario_arrivo)
                               """, valori)
            connection.commit()

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Errore durante l'esecuzione della query: {e}")
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
                "icao24": row[1],
                "icao_partenza": row[2],
                "icao_arrivo": row[3],
                "orario_partenza" : row[4],
                "orario_arrivo" : row[5]
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

@app.route('/last_flights', methods=['GET'])
def get_last_flights():
    connection = get_db_connection()
    cursor = connection.cursor()
    aeroporto = request.args.get('cod_aeroporto')
    user_email = request.args.get('user_email')

    if not aeroporto or not user_email:
        return jsonify({"error": "Parametri 'cod_aeroporto' e 'user_id' mancanti."}), 400

    risultati_finali = {
        "ultimo_volo_in_partenza": None,
        "ultimo_volo_in_arrivo": None
    }

    try:
        check_interest_query = """
                               SELECT 1
                               FROM interests
                               WHERE email_user = %s AND cod_aeroporto = %s \
                               """
        cursor.execute(check_interest_query, (user_email, aeroporto))
        interest_exists = cursor.fetchone()

        if not interest_exists:
            return jsonify({
                "error": "Accesso negato: l'utente non ha registrato questo aeroporto come interesse."
            }), 403 # Status code 403 Forbidden

        select_partenza_query = """
                                SELECT id,icao_24, icao_partenza, icao_arrivo, orario_partenza, orario_arrivo
                                FROM flights
                                WHERE icao_partenza=%s
                                ORDER BY orario_partenza DESC
                                    LIMIT 1 \
                                """
        cursor.execute(select_partenza_query, (aeroporto,))
        partenza_row = cursor.fetchone()

        if partenza_row:
            risultati_finali["ultimo_volo_in_partenza"] = {
                "id": partenza_row[0],
                "icao_24": partenza_row[1],
                "icao_partenza": partenza_row[2],
                "icao_arrivo": partenza_row[3],
                "orario_partenza" : partenza_row[4],
                "orario_arrivo" : partenza_row[5]
            }

        select_arrivo_query = """
                              SELECT id,icao_24, icao_partenza, icao_arrivo, orario_partenza, orario_arrivo
                              FROM flights
                              WHERE icao_arrivo=%s
                              ORDER BY orario_arrivo DESC
                                  LIMIT 1 \
                              """
        cursor.execute(select_arrivo_query, (aeroporto,))
        arrivo_row = cursor.fetchone()

        if arrivo_row:
            risultati_finali["ultimo_volo_in_arrivo"] = {
                "id": arrivo_row[0],
                "icao_24": arrivo_row[1],
                "icao_partenza": arrivo_row[2],
                "icao_arrivo": arrivo_row[3],
                "orario_partenza" : arrivo_row[4],
                "orario_arrivo" : arrivo_row[5]
            }
        return jsonify(risultati_finali), 200

    except Exception as e:
        print(f"Errore durante l'esecuzione della query: {e}")
        return jsonify({
            "error": "Errore interno del server durante il recupero dei voli."
        }), 500
    finally:
        cursor.close()
        connection.close()

@app.route('/avg_flights', methods=['GET'])
def get_avg_flights():
    connection = get_db_connection()
    cursor = connection.cursor()
    aeroporto = request.args.get('cod_aeroporto')
    user_email = request.args.get('user_email')

    try:
        days_num = int(request.args.get('num_giorni'))
        days_num_str = str(days_num)
    except (TypeError, ValueError):
        return jsonify({"error": "Parametro 'num_giorni' mancante o non valido."}), 400

    if not aeroporto or not user_email:
        return jsonify({"error": "Parametri 'cod_aeroporto' e 'user_email' mancanti."}), 400

    try:
        check_interest_query = """
                               SELECT 1
                               FROM interests
                               WHERE email_user = %s AND cod_aeroporto = %s \
                               """
        cursor.execute(check_interest_query, (user_email, aeroporto))
        interest_exists = cursor.fetchone()

        if not interest_exists:
            return jsonify({
                "error": "Accesso negato: l'utente non ha registrato questo aeroporto come interesse."
            }), 403


        check_avg_partenze_query = f"""
                          SELECT
                              COUNT(id) / {days_num_str} AS media_giornaliera
                          FROM
                              flights
                          WHERE
                              icao_partenza = %s
                            AND orario_partenza >= DATE_SUB(NOW(), INTERVAL %s DAY);
                               """
        cursor.execute(check_avg_partenze_query, (aeroporto, days_num_str))
        avg_partenze = cursor.fetchone()[0]

        check_avg_arrivi_query = f"""
                                   SELECT
                                       COUNT(id) / {days_num_str} AS media_giornaliera
                                   FROM
                                       flights
                                   WHERE
                                       icao_arrivo = %s
                                     AND orario_arrivo >= DATE_SUB(NOW(), INTERVAL %s DAY);
                                   """
        cursor.execute(check_avg_arrivi_query, (aeroporto, days_num_str))
        avg_arrivi = cursor.fetchone()[0]

        return jsonify({
            "cod_aeroporto": aeroporto,
            "media_calcolata_su_giorni": days_num,
            "risultati": {
                "media_voli_partenza": float(avg_partenze),
                "media_voli_arrivo": float(avg_arrivi)
            },
            "messaggio": "Media dei voli storici calcolata con successo."
        }), 200

    except Exception as e:
        print(f"Errore durante l'esecuzione della query: {e}")
        return jsonify({
            "error": "Errore interno del server durante il recupero dei voli."
        }), 500
    finally:
        cursor.close()
        connection.close()

def start_scheduler():
    time.sleep(3)
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        func=get_data_scheduler,
        trigger='interval',
        hours=12,
        #minutes= 2,
        id='volo_data_fetcher',  # ID univoco per il job
        name='Recupero Dati Voli OpenSky',
        replace_existing=True
    )
    scheduler.start()
    print("Scheduler APS avviato. Il job 'get_data_scheduler' verrà eseguito ogni 12 ore.")

if __name__ == '__main__':
    init_db()
    start_scheduler()
    app.run(host='0.0.0.0', port=5000)
