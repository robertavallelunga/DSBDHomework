import time
import threading
import json
import os
import requests
from flask import Flask, jsonify, request
from kafka import KafkaProducer

app = Flask(__name__)

PROMETHEUS_URL = os.getenv('PROMETHEUS_URL', 'http://prometheus:9090')
KAFKA_HOST = os.getenv('KAFKA_HOST', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'sla-breach-topic')
SLA_CONFIG_FILE = 'sla_config.json'

# REQUISITO: CHECK_INTERVAL= T_check >= 5 * T_scrape. T_scape è 15 sec
# Leggiamo la variabile d'ambiente 'CHECK_INTERVAL'. Se non c'è, default a 80.
# Usiamo int() perché os.getenv restituisce una stringa e time.sleep vuole un numero.
CHECK_INTERVAL = int(os.getenv('CHECK_INTERVAL', '80'))


# Memoria interna per i dati
sla_status = {} #Contiene i dati in tempo reale (es. "Ultimi 10 valori letti").
sla_config = [] #Contiene le regole (es. "Max 5 secondi").
breach_history = []


# --- INIZIALIZZAZIONE KAFKA ----
def init_producer():
    """Prova a connettersi a Kafka e restituisce il producer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_HOST,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("[SlaManager] Kafka Producer connesso con successo.")
        return producer
    except Exception as e:
        print(f"[SlaManager] ATTENZIONE: Impossibile connettersi a Kafka: {e}")
        return None


# --- GESTIONE CONFIGURAZIONE ---
def load_config():
    global sla_config, sla_status
    try:
        with open(SLA_CONFIG_FILE, 'r') as f:
            sla_config = json.load(f) #json.load trasforma il contenuto del file in una lista di dizionari

        for rule in sla_config: #per ogni regola che dal file json è stata spostata in sla_config
            name = rule.get('nome_metrica')
            if name and name not in sla_status: #Se ho trovato un nome valido e il nome non è ancora nella memoria di stato sla_status
                sla_status[name] = {"tempi_rilevati": [], "breach_count": 0}
        print(f"[SlaManager] Config caricata: {len(sla_config)} regole.")
    except Exception as e:
        print(f"[SlaManager] Errore caricamento config: {e}")


# --- LOOP DI MONITORAGGIO ---
def monitor_loop(producer):
    print(f"[SlaManager] Avvio monitoraggio ogni {CHECK_INTERVAL} secondi...")

    while True:
        try:
            # Ricarichiamo il producer se è caduto
            if not producer:
                producer = init_producer()

            current_config = list(sla_config)

            for rule in current_config:
                metric_name = rule.get('nome_metrica')
                query = rule.get('queryPromQL')

                min_val = rule.get('min', float('-inf'))
                max_val = rule.get('max', float('inf'))

                if not metric_name or not query:
                    continue

                try:
                    # 1. Chiede a Prometheus
                    response = requests.get(f"{PROMETHEUS_URL}/api/v1/query", params={'query': query})
                    data = response.json()

                    #Prima di leggere, controlliamo se Prometheus ha risposto "success" e se ci sono dati (result)
                    if data.get('status') == 'success' and data.get('data', {}).get('result'):
                        # Nella risposta (data), vai nella sezione data, guarda i result, prendi il primo [0], guarda il suo value, ignora il tempo e prendi il secondo elemento [1], e infine trasformalo in numero (float)
                        current_val = float(data['data']['result'][0]['value'][1])

                        # Inizializzazione sicura
                        if metric_name not in sla_status:
                            sla_status[metric_name] = {"tempi_rilevati": [], "breach_count": 0}
                        #creo un riferimento
                        status = sla_status[metric_name]

                        # 2. Aggiorna storico (ultimi 10 valori)
                        status['tempi_rilevati'].append(current_val)
                        #Se la lista è diventata troppo lunga (più di 10 numeri), cancelliamo il primo della lista (il più vecchio)
                        if len(status['tempi_rilevati']) > 10:
                            status['tempi_rilevati'].pop(0)

                        # 3. Controllo SLA (3 violazioni)
                        valori_recenti = status['tempi_rilevati']
                        #Scorri la lista e conta quante volte trovi un numero che supera il limite.
                        over_max = sum(1 for valore in valori_recenti if valore > max_val)
                        under_min = sum(1 for valore in valori_recenti if valore < min_val)

                        condition = None
                        if over_max >= 3:
                            condition = f"High Breach (> {max_val})"
                        elif under_min >= 3:
                            condition = f"Low Breach (< {min_val})"

                        if condition:
                            # 4. Invia Allarme

                            #Aumenta di 1 il contatore delle violazioni totali per questa metrica
                            status['breach_count'] += 1
                            event = {
                                "timestamp": time.time(),
                                "metric": metric_name,
                                "value_observed": current_val,
                                "threshold_violated": condition,
                                "total_violations": status['breach_count']
                            }
                            #Salva questo verbale anche nella memoria RAM del programma
                            breach_history.append(event)

                            print(f"[ALLARME SLA] {metric_name}: {condition}")

                            if producer:
                                producer.send(KAFKA_TOPIC, event)
                                producer.flush()
                            else:
                                print("[SlaManager] Impossibile inviare a Kafka (Producer non pronto).")

                            # Reset dopo l'allarme
                            #Cancella la lista degli ultimi 10 valori
                            status['tempi_rilevati'] = []

                except Exception as e:
                    print(f"[SlaManager] Errore metrica {metric_name}: {e}")

        except Exception as e:
            print(f"[SlaManager] Errore loop: {e}")
        # Aspettiamo tra un controllo e l'altro
        time.sleep(CHECK_INTERVAL)


# --- API REST ---

#Read SLA config: legge configurazione corrente
@app.route('/sla/config', methods=['GET'])
def get_config():
    return jsonify(sla_config)

#Update SLA config: aggiorna le soglie/metriche
@app.route('/sla/config', methods=['POST'])
def update_config():
    global sla_config
    new_conf = request.json
    try:
        with open(SLA_CONFIG_FILE, 'w') as f:
            #scrivo in f le new_conf su 4 righe diverse come nel file originale
            json.dump(new_conf, f, indent=4)
        #richiamo load_config() perché il file è stato modificato e va riletto
        load_config()
        return jsonify({"status": "updated", "config": new_conf})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


#Breach stats: restituisce quali metriche hanno avuto breach e quanti, dall’avvio del servizio
@app.route('/sla/stats', methods=['GET'])
def get_stats():

    riassunto_breach = {}

    for nome_metrica, dati in sla_status.items():
        # Controlliamo il contatore 'breach_count' che aggiorniamo nel loop
        totale_breach = dati['breach_count']
        # Se il totale è 0, tecnicamente non "ha avuto" breach, quindi lo escludiamo
        if totale_breach > 0:
            riassunto_breach[nome_metrica] = totale_breach

    return jsonify({
        "info": "Totale violazioni SLA dall'avvio del servizio",
        "breaches": riassunto_breach
    })


# --- AVVIO DEL PROGRAMMA ---
if __name__ == '__main__':
    # 1. Carichiamo la configurazione iniziale
    load_config()

    # 2. Inizializziamo il producer Kafka
    kafka_producer = init_producer()

    # 3. Avviamo il Loop di Monitoraggio in background (Thread separato)
    # daemon=True significa che se il programma principale si chiude, muore anche questo thread
    t = threading.Thread(target=monitor_loop, args=(kafka_producer,), daemon=True)
    t.start()

    # 4. Avviamo il server Web Flask (questo blocca il programma qui in ascolto)
    print("[SlaManager] Server Flask in avvio sulla porta 5000...")
    app.run(host='0.0.0.0', port=5000)