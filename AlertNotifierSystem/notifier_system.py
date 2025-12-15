import json
import os
import smtplib
import sys
import time
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
kafka_consumer = None

def init_consumer():
    global kafka_consumer
    if kafka_consumer:
        return kafka_consumer

    try:
        consumer = KafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=KAFKA_HOST,
            client_id='Notifier-Consumer',
            group_id='notifier-group',
            max_poll_records=500,
            fetch_max_wait_ms=500,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            # 1010101010101010 => deserializzazione => Ciao
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
    time.sleep(15)
    consumer = init_consumer()

    for message in consumer:
        alert = message.value
        email = alert.get('email')
        airport = alert.get('airport')
        condition = alert.get('condition')

        if email and condition:
            oggetto = f"Alert Voli: {airport}"
            body = (f"Salve,\n\n"
                    f"Il sistema ha rilevato una condizione di superamento soglia per l'aeroporto {airport}.\n"
                    f"Dettaglio: {condition}\n\n"
                    f"Saluti,\nDSBD Team")

            send_email(email, oggetto, body)

if __name__ == "__main__":
    start_notifier()