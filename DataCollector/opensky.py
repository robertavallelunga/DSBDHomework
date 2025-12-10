import time
import requests

TOKEN = ""

def token():
    CLIENT_ID = "zantoniix-api-client"
    CLIENT_SECRET = "RLDx6qz7C0aWHWtsVViPzmDyejC05t6j"
    #{"clientId":"ro30by-api-client","clientSecret":"XIgDS7tLe7dajKblb7ddINgfYK0g0G8L"}

    global TOKEN

    URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    try:
        response = requests.post(URL, data=payload, headers=headers)
        response.raise_for_status()
        access_token = response.json().get("access_token")

        if access_token:
            #print(access_token)
            TOKEN = access_token
        else:
            print("Errore: Impossibile estrarre il token.")

    except requests.exceptions.RequestException as e:
        print(f"Errore di richiesta: {e}")

def voli_arrivo(icao):
    global TOKEN
    # raise requests.exceptions.RequestException("Simulazione API Down per Test")      TEST CIRCUIT BREAKER
    URL = "https://opensky-network.org/api/flights/arrival"
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Accept": "application/json"
    }

    end_time = int(time.time())
    begin_time = end_time - 43200

    params = {
        "airport": f"{icao}",
        "begin": begin_time,
        "end": end_time
    }

    print(f"Effettuando una richiesta a: {URL}...")

    response = requests.get(URL, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    print("\n Dati recuperati con successo:")
    return data

def voli_partenza(icao):
    global TOKEN
    # raise requests.exceptions.RequestException("Simulazione API Down per Test")   TEST CIRCUIT BREAKER
    URL = "https://opensky-network.org/api/flights/departure"
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Accept": "application/json"
    }

    end_time = int(time.time())
    begin_time = end_time - 43200

    params = {
        "airport": f"{icao}",
        "begin": begin_time,
        "end": end_time
    }

    print(f"Effettuando una richiesta a: {URL}...")

    response = requests.get(URL, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    print("\n Dati recuperati con successo:")
    return data