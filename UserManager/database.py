import os
import time
import mysql.connector

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': 'root',
    'password': os.getenv('MYSQL_ROOT_PASSWORD', 'pasquale'), # password dal compose
    'database': os.getenv('DB_NAME', 'mysqluserDB')
}

def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)

def init_db():
    # Attesa per l'avvio del DB container
    time.sleep(10)
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
        cursor.close()
        conn.close()
        print("Tabella 'users' inizializzata con successo.")
    except Exception as e:
        print(f"Errore durante l'init del DB: {e}")