import os
import time
import mysql.connector

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': 'root',
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