import os
import time
import mysql.connector

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': 'root',
    #'port': '3310',
    'password': os.getenv('MYSQL_ROOT_PASSWORD', 'ermenegildo'), # password dal compose
    'database': os.getenv('DB_NAME', 'mysqldataDB'),
    'pool_name':"data_pool",
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
                # Creiamo la piscina con 5 connessioni pronte all'uso
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