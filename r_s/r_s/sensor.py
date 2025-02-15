import psycopg2
from datetime import datetime
import os
from dotenv import load_dotenv
load_dotenv('/home/repo/ML/dagster-rs/r_s/r_s/.env') 

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
            all_tables_updated = False  # No hay datos en la tabla
            continue

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
                INSERT INTO last_updated_1.last_extracted_at (table_name, last_extracted_at) 
                VALUES (%s, %s);
            """
            cursor.execute(query, (table, max_extracted_at))
            all_tables_updated = False  # Hubo un cambio (nuevo registro)
        else:
            last_extracted_at = result[0]
            if max_extracted_at > last_extracted_at:
                # Hay cambios, actualizar el valor en `last_updated_1.last_extracted_at`
                query = """
                    UPDATE source.last_updated_1
                    SET last_extracted_at = %s 
                    WHERE table_name = %s;
                """
                cursor.execute(query, (max_extracted_at, table))
                all_tables_updated = False  # Hubo un cambio (actualización)
            else:
                # La tabla está actualizada
                pass

    conn.commit()
    cursor.close()
    conn.close()

    # Retorna True solo si todas las tablas están actualizadas
    return all_tables_updated    