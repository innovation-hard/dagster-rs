import psycopg2
from datetime import datetime
import os
from dotenv import load_dotenv
load_dotenv('/home/repo/ML/dagster-rs/r_s/r_s/.env') 

from datetime import datetime, timedelta

import logging
# Configurar el logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("sensor.log"),  # Escribir en un archivo
        logging.StreamHandler()  # Escribir en la consola
    ]
)

def round_to_minute(timestamp):
    """
    Redondea un timestamp al minuto más cercano.
    Si el timestamp es `timestamptz`, lo convierte a `timestamp` sin zona horaria antes de redondear.
    """
    if timestamp is None:
        return None
    
    # Si el timestamp tiene zona horaria, lo convertimos a timestamp sin zona horaria
    if timestamp.tzinfo is not None:
        timestamp = timestamp.replace(tzinfo=None)
    
    # Redondear al minuto más cercano
    return timestamp.replace(second=0, microsecond=0)

def check_for_changes_in_postgres():
    """
    Verifica si hay cambios en las tablas del esquema `source` de PostgreSQL.
    Retorna True si hay cambios en alguna tabla, False en caso contrario.
    """
    conn = psycopg2.connect(
        dbname='mlops', #os.getenv("POSTGRES_DB"),
        user='airbyte', ##os.getenv("POSTGRES_USER"),
        password='airbyte', #os.getenv("POSTGRES_PASSWORD"),
        host='localhost', #os.getenv("POSTGRES_HOST"),
        port=5432, #os.getenv("POSTGRES_PORT"),
    )
    
    cursor = conn.cursor()
    # Tablas a verificar
    tables = ["movies", "scores", "users"]
    all_tables_updated = True  # Asumimos que todas las tablas están actualizadas

    for table in tables:
        # Obtener el valor máximo de `last_extracted_at` para la tabla actual
        query = f"""
            SELECT MAX(_airbyte_extracted_at) 
            FROM source.{table};
        """
        cursor.execute(query)
        max_extracted_at = cursor.fetchone()[0]

        if max_extracted_at is None:
            logging.warning(f"No hay datos en la tabla {table}.")
            all_tables_updated = False  # No hay datos en la tabla
            continue

        # Redondear el timestamp al minuto más cercano
        max_extracted_at = round_to_minute(max_extracted_at)
        logging.info(f"Tabla: {table}, max_extracted_at (redondeado): {max_extracted_at}")

        # Obtener el último valor registrado en `last_updated_1.last_extracted_at`
        query = """
            SELECT last_extracted_at 
            FROM source.last_updated_1 
            WHERE table_name = %s;
        """
        cursor.execute(query, (table,))
        result = cursor.fetchone()

        if result is None:
            # No hay registro previo, insertar el nuevo valor
            query = """
                INSERT INTO source.last_updated_1 (table_name, last_extracted_at) 
                VALUES (%s, %s);
            """
            cursor.execute(query, (table, max_extracted_at))
            logging.info(f"Nuevo registro insertado para la tabla {table}.")
            all_tables_updated = False  # Hubo un cambio (nuevo registro)
        else:
            last_extracted_at = round_to_minute(result[0])
            logging.info(f"Tabla: {table}, last_extracted_at (redondeado): {last_extracted_at}")

            if max_extracted_at > last_extracted_at:
                # Hay cambios, actualizar el valor en `last_updated_1.last_extracted_at`
                query = """
                    UPDATE source.last_updated_1
                    SET last_extracted_at = %s 
                    WHERE table_name = %s;
                """
                cursor.execute(query, (max_extracted_at, table))
                logging.info(f"Cambio detectado en la tabla {table}. Actualizando last_extracted_at.")
                all_tables_updated = False  # Hubo un cambio (actualización)
            else:
                # La tabla está actualizada
                 logging.info(f"Tabla {table} está actualizada.")


    # Cerrar la conexión
    conn.commit()
    cursor.close()
    conn.close()

    # Retorna True solo si todas las tablas están actualizadas
    logging.info(f"Resultado final: {'Todas las tablas están actualizadas' if all_tables_updated else 'Cambios detectados en alguna tabla'}.")

    # Retorna True solo si todas las tablas están actualizadas
    return not all_tables_updated    