import os
import time
import mysql.connector

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': 'root',
    #'port': '3310',
    'password': os.getenv('MYSQL_ROOT_PASSWORD', 'ermenegildo'), # password dal compose
    'database': os.getenv('DB_NAME', 'mysqldataDB')
}

def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)

def init_db():
    # Attesa per l'avvio del DB container
    time.sleep(20)
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Creazione Tabella Interessi
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS interests (
                           id INT AUTO_INCREMENT PRIMARY KEY,
                           email_user VARCHAR(255),
                           cod_aeroporto VARCHAR(10),
                           UNIQUE (email_user, cod_aeroporto)
                           )
                       """)
        conn.commit()
        cursor.close()
        conn.close()
        print("Tabella 'interests' inizializzata con successo.")
    except Exception as e:
        print(f"Errore durante l'init del DB: {e}")