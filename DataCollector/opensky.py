import time
import requests
import json

TOKEN = ""

def token():
    # Definisci le tue credenziali
    CLIENT_ID = "zantoniix-api-client"       # <-- Modifica
    CLIENT_SECRET = "RLDx6qz7C0aWHWtsVViPzmDyejC05t6j" # <-- Modifica
    global TOKEN


    # Imposta l'endpoint e i dati della richiesta
    URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    # Esegui la richiesta e ottieni il token
    try:
        response = requests.post(URL, data=payload, headers=headers)
        response.raise_for_status()

        # Estrai e stampa il token, come faceva 'jq'
        access_token = response.json().get("access_token")

        if access_token:
            # Questo è il risultato finale snellito
            print(access_token)
            TOKEN = access_token
        else:
            # Gestione minima dell'errore se il token non è presente
            print("Errore: Impossibile estrarre il token.")

    except requests.exceptions.RequestException as e:
        # Gestione di errori di connessione o HTTP
        print(f"Errore di richiesta: {e}")

def voli():
    global TOKEN
    URL = "https://opensky-network.org/api/flights/arrival"
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Accept": "application/json" # È buona prassi specificare che ci si aspetta JSON
    }

    end_time = int(time.time())
    begin_time = end_time - 43200

    params = {
        "airport": "LIMC",
        "begin": begin_time,
        "end": end_time
    }

    # 3. Esecuzione della Richiesta GET
    try:
        print(f"Effettuando una richiesta a: {URL}...")

        response = requests.get(URL, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        print("\n Dati recuperati con successo:")
        print(json.dumps(data, indent=4))

    except requests.exceptions.RequestException as e:
        # Gestione degli errori di rete o di stato (4xx/5xx)
        print(f"\nErrore durante la richiesta API: {e}")
        if response.status_code == 401:
            print("Verifica che il tuo TOKEN sia corretto e valido.")

    except json.JSONDecodeError:
        # Gestione degli errori di decodifica JSON
        print("\nErrore nella decodifica JSON della risposta.")
        print("Contenuto della risposta:\n", response.text)