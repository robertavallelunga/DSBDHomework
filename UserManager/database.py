import os
import time
import mysql.connector

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': 'root',
    #'port': '3311',
    'password': os.getenv('MYSQL_ROOT_PASSWORD', 'pasquale'), # password dal compose
    'database': os.getenv('DB_NAME', 'mysqluserDB'),
    'pool_name':"user_pool",
    'pool_size':10, # Numero di connessioni sempre attive
    'pool_reset_session':True
}

connection_pool = None

def get_db_connection():
    global connection_pool

    if connection_pool is None:
        retries = 5
        while retries > 0:
            try:
                # Prova di connessione con 5 tentativi
                connection_pool = mysql.connector.pooling.MySQLConnectionPool(**DB_CONFIG)
                break
            except mysql.connector.Error as err:
                print(f"Errore: {err}")
                print(f"Riprovo tra 5 secondi... ({retries} rimasti)")
                time.sleep(5)
                retries -= 1

        if connection_pool is None:
            raise Exception("Impossibile connettersi al database (Pool creation failed).")

    try:
        connection = connection_pool.get_connection()
        return connection
    except Exception as e:
        print(f"[ERROR] DB: Impossibile ottenere connessione dal pool: {e}")
        raise e

def init_db():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Creazione Tabella Utenti
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS users (
                           email VARCHAR(255) PRIMARY KEY,
                           nome VARCHAR(100),
                           cognome VARCHAR(100)
                           )
                       """)
        conn.commit()

        # Creazione tabella per le richieste di registrazione
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS requestID (
                           messageID VARCHAR(100) PRIMARY KEY,
                           response VARCHAR(255)
                           )
                       """)
        conn.commit()
        cursor.close()
        conn.close()
        print("Tabelle 'users' e 'requestID' inizializzate con successo.")
    except Exception as e:
        print(f"Errore durante l'init del DB: {e}")