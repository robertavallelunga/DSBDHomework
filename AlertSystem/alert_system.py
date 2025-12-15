import os
import time

from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from database import get_db_connection

import json

KAFKA_HOST = os.getenv('KAFKA_HOST', 'kafka:9092')
INPUT_TOPIC = os.getenv('INPUT_TOPIC', 'to-alert-system')
OUTPUT_TOPIC = os.getenv('OUTPUT_TOPIC', 'to-notifier')

def init_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_HOST,    # Indirizzo broker nel Docker
            client_id='AlertSystem-Producer',    # <--- Un ID unico
            batch_size=16384,
            linger_ms=50,
            max_in_flight_requests_per_connection=1,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(0, 10)
        )
        print("[KAFKA] Connesso con successo.")
        return producer
    except NoBrokersAvailable:
        print("[KAFKA] Broker non disponibile. Riproverò alla prossima chiamata.")
        return None
    except Exception as e:
        print(f"[KAFKA] Errore generico di connessione: {e}")
        return None

def init_consumer():
    try:
        consumer = KafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=KAFKA_HOST,
            client_id='AlertSystem-Consumer',
            api_version=(0, 10),
            group_id='alert-system-group',
            max_poll_records=500,
            fetch_max_wait_ms=500,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            enable_auto_commit=False
        )
        print("[KAFKA] Connesso con successo.")
        return consumer
    except NoBrokersAvailable:
        print("[KAFKA] Broker non disponibile. Riproverò alla prossima chiamata.")
        return None
    except Exception as e:
        print(f"[KAFKA] Errore generico di connessione: {e}")
        return None

def get_interested_users(airport_code):
    """Interroga il DB per trovare a chi interessa questo aeroporto"""
    users = []
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        query = "SELECT email_user, highValue, lowValue FROM interests WHERE cod_aeroporto = %s"
        cursor.execute(query, (airport_code,))

        for (email, hv, lv) in cursor.fetchall():
            users.append({
                'email': email,
                'high_value': hv if hv is not None else -1,
                'low_value': lv if lv is not None else -1
            })
        cursor.close()
    except Exception as e:
        print(f"[AlertSystem] Errore DB: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()
    return users

def start_alert_system():
    print("[AlertSystem] Inizializzazione... Attesa broker Kafka.")

    producer = None
    while producer is None:
        producer = init_producer()
        if not producer:
            print("[KAFKA] Producer non pronto, riprovo tra 5 secondi...")
            time.sleep(5)

    consumer = None
    while consumer is None:
        consumer = init_consumer()
        if not consumer:
            print("[KAFKA] Consumer non pronto, riprovo tra 5 secondi...")
            time.sleep(5)

    print("[AlertSystem] Sistema pronto. In ascolto...")

    try:
        # Loop principale sui messaggi in arrivo
        for message in consumer:
            data = message.value
            icao = data.get('icao')
            arrivi = data.get('arrivi', 0)
            partenze = data.get('partenze', 0)
            totale = arrivi + partenze

            interested_users = get_interested_users(icao)

            if not interested_users:
                print(f"[AlertSystem] Nessun utente interessato a {icao}. Skip.")
            else:
                # --- LOGICA CORRETTA: Ciclo sugli utenti ---
                for user in interested_users:
                    email = user['email']
                    hv = user['high_value']
                    lv = user['low_value']
                    condition_msg = None

                    # Verifica Soglie
                    if hv > 0 and totale > hv:
                        condition_msg = f"SOGLIA SUPERATA: Voli ({totale}) > Soglia Alta ({hv})"
                    elif lv > 0 and totale < lv:
                        condition_msg = f"SOGLIA SUPERATA: Voli ({totale}) < Soglia Bassa ({lv})"

                    # Invio Alert se necessario
                    if condition_msg:
                        alert_payload = {
                            "email": email,
                            "icao": icao,
                            "condition": condition_msg
                        }
                        producer.send(OUTPUT_TOPIC, alert_payload)
                        print(f"[AlertSystem] -> Notifica per {email}: {condition_msg}")
            # Confermiamo a Kafka che abbiamo finito di processare questo messaggio
            # per TUTTI gli utenti interessati.
            try:
                consumer.commit()
            except Exception as e:
                print(f"[KAFKA] Errore durante il commit: {e}")

    except Exception as e:
        print(f"[AlertSystem] Errore critico nel loop: {e}")
    finally:
        if consumer: consumer.close()
        if producer:
            producer.flush()
            producer.close()




if __name__ == "__main__":
    start_alert_system()