import json
import os
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# Configurazione SMTP
SMTP_HOST = os.getenv('SMTP_HOST', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
SMTP_USER = os.getenv('SMTP_USER')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD')
SENDER_NAME = os.getenv('SENDER_NAME', 'DSBD Alert System')

# Configurazione Kafka
KAFKA_HOST = os.getenv('KAFKA_HOST', 'kafka:9092')
INPUT_TOPIC = os.getenv('INPUT_TOPIC', 'to-notifier')

def init_consumer():
    try:
        consumer = KafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=KAFKA_HOST,
            client_id='Notifier-Consumer',
            group_id='notifier-group',
            max_poll_records=500,
            fetch_max_wait_ms=500,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            enable_auto_commit=False
        )
        kafka_consumer = consumer
        print("[KAFKA] Connesso con successo.")
        return kafka_consumer
    except NoBrokersAvailable:
        print("[KAFKA] Broker non disponibile. Riproverò alla prossima chiamata.")
        return None
    except Exception as e:
        print(f"[KAFKA] Errore generico di connessione: {e}")
        return None

def send_email(email, oggetto, body):
    if not SMTP_USER or not SMTP_PASSWORD:
        print("[AlertNotifier] ERRORE: Credenziali SMTP mancanti.")
        return

    msg = MIMEMultipart()
    msg['From'] = f"{SENDER_NAME} <{SMTP_USER}>"
    msg['To'] = email
    msg['Subject'] = oggetto
    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, email, msg.as_string())
        server.quit()
        print(f"[AlertNotifier] Email inviata a {email}")
    except Exception as e:
        print(f"[AlertNotifier] Errore invio email: {e}")

def start_notifier():
    print("[AlertSystem] Inizializzazione... Attesa broker Kafka.")
    consumer = None
    while consumer is None:
        consumer = init_consumer()
        if consumer is None:
            print("In attesa di Kafka...")
            time.sleep(5)

    print("[Notifier] In attesa di messaggi...")

    for message in consumer:
        alert = message.value
        email = alert.get('email')
        airport = alert.get('icao')
        condition = alert.get('condition')
        oggetto = ""
        body = ""

        # Alert SLA
        if 'metric' in alert:
            metrica = alert.get('metric')
            soglia = alert.get('threshold_violated')
            valore = alert.get('value_observed')
            timestamp = alert.get('timestamp', time.time())
            # Trasformiamo il timestamp in una data leggibile
            ts_leggibile = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
            violazioni_totali = alert.get('total_violations', 1)

            oggetto = f"Violazione SLA su {metrica}"
            body = (
                f"Salve Admin,\n\n"
                f"Il sistema di monitoraggio ha rilevato un problema critico.\n"
                f"------------------------------------------------\n"
                f"DATA/ORA:       {ts_leggibile}\n"
                f"METRICA: {metrica}\n"
                f"SOGLIA VIOLATA:  {soglia}\n"
                f"VALORE ATTUALE: {valore}\n"
                f"NUMERO VIOLAZIONI REGISTRATE: {violazioni_totali}\n"
                f"------------------------------------------------\n\n"
                f"Si prega di intervenire.\n"
                f"DSBD Monitoring System"
            )
            send_email(email, oggetto, body)


        # Alert Voli
        elif 'icao' in alert:
            oggetto = f"Alert Voli: {airport}"
            body = (f"Salve,\n\n"
                    f"Il sistema ha rilevato una condizione di superamento soglia per l'aeroporto {airport}.\n"
                    f"Dettaglio: {condition}\n\n"
                    f"Saluti,\nDSBD Team")

            send_email(email, oggetto, body)

        consumer.commit()
if __name__ == "__main__":
    start_notifier()