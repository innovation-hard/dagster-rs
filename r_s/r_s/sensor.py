import psycopg2
from datetime import datetime
import os
from dotenv import load_dotenv
load_dotenv('/home/repo/ML/dagster-rs/r_s/r_s/.env') 

def check_for_changes_in_postgres():
    """
    Verifica si hay cambios en las tablas del esquema `source` de PostgreSQL.
    Retorna True si hay cambios, False en caso contrario.
    """
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
    )
    cursor = conn.cursor()

    # Tablas a verificar
    tables = ["movies", "scores", "users"]
    changes_detected = False

    for table in tables:
        # Obtener el valor máximo de `_airbyte_extracted_at` para la tabla actual
        query = f"""
            SELECT MAX(_airbyte_extracted_at) 
            FROM source.{table};
        """
        cursor.execute(query)
        max_extracted_at = cursor.fetchone()[0]

        if max_extracted_at is None:
            continue  # No hay datos en la tabla

        # Obtener el último valor registrado en `.last_extracted`
        query = """
            SELECT last_extracted_at 
            FROM metadata.last_extracted 
            WHERE table_name = %s;
        """
    cursor.execute(query)
    result = cursor.fetchone()

    if result is None:
        # No hay registro previo, insertar el nuevo valor
        query = """
            INSERT INTO metadata.last_extracted (table_name, last_extracted_at) 
            VALUES (%s, %s);
        """
        cursor.execute(query, (table, max_extracted_at))
        changes_detected = True
    else:
        last_extracted_at = result[0]
        if max_extracted_at > last_extracted_at:
            # Hay cambios, actualizar el valor en `metadata.last_extracted`
            query = """
                UPDATE metadata.last_extracted 
                SET last_extracted_at = %s 
                WHERE table_name = %s;
            """
            cursor.execute(query, (max_extracted_at, table))
            changes_detected = True

    conn.commit()
    cursor.close()
    conn.close()

    # Si hay nuevas filas, retorna True
    return changes_detected