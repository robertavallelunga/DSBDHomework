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
                           highValue INTEGER,
                           lowValue INTEGER,
                           UNIQUE (email_user, cod_aeroporto),
                           CHECK (highValue IS NULL OR highValue > lowValue)
                           )
                       """)
        conn.commit()

        # Creazione Tabella Voli
        cursor.execute("""
               CREATE TABLE IF NOT EXISTS flights (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    icao_24 VARCHAR(20) NOT NULL,
                   icao_partenza VARCHAR(10),
                   icao_arrivo VARCHAR(10),
                   orario_partenza DATETIME NOT NULL,
                   orario_arrivo DATETIME,
                   
                   UNIQUE KEY unique_aircraft_flight_start (
                        icao_24,
                        orario_partenza
                                                           )
                   )
               """)
        conn.commit()
        cursor.close()
        conn.close()
        print("Tabella 'interests' inizializzata con successo.")
    except Exception as e:
        print(f"Errore durante l'init del DB: {e}")