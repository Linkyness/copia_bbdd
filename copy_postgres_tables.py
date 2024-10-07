import os
import yaml
import psycopg2
from psycopg2 import sql
import argparse

# Cargar el archivo de configuración YAML
def load_config(config_file):
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

# Conectar a la base de datos PostgreSQL
def connect_to_db(host, port, user, password, database='postgres'):
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )
    return conn

# Verificar si la tabla existe en la base de datos de destino
def table_exists(dest_conn, dest_schema, dest_table):
    with dest_conn.cursor() as dest_cursor:
        dest_cursor.execute(sql.SQL("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            );
        """), [dest_schema, dest_table])
        return dest_cursor.fetchone()[0]

# Obtener el esquema de la tabla de origen
def get_table_schema(source_conn, source_schema, source_table):
    with source_conn.cursor() as source_cursor:
        source_cursor.execute(sql.SQL("""
            SELECT column_name, data_type, is_nullable, character_maximum_length 
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s;
        """), [source_schema, source_table])
        return source_cursor.fetchall()

# Crear la tabla en la base de datos de destino con el esquema de origen
def create_table(dest_conn, dest_schema, dest_table, schema):
    with dest_conn.cursor() as dest_cursor:
        create_query = sql.SQL("""
            CREATE TABLE {}.{} (
                {}
            );
        """).format(
            sql.Identifier(dest_schema),
            sql.Identifier(dest_table),
            sql.SQL(', ').join(
                sql.SQL("{} {} {}").format(
                    sql.Identifier(col[0]),  # column_name
                    sql.SQL(col[1]),  # data_type
                    sql.SQL("NULL" if col[2] == "YES" else "NOT NULL")  # is_nullable
                )
                for col in schema
            )
        )
        dest_cursor.execute(create_query)
        print(f"Tabla {dest_schema}.{dest_table} creada con éxito.")

# Función para copiar tablas (total o incremental) en bloques de 10,000
def copy_table(source_conn, dest_conn, source_schema, source_table, dest_schema, dest_table, copy_type, incremental_column=None, full_refresh=False, batch_size=10000):
    # Verificar si la tabla existe en la base de datos de destino, si no existe, crearla
    if not table_exists(dest_conn, dest_schema, dest_table):
        print(f"La tabla {dest_schema}.{dest_table} no existe. Creando la tabla...")
        schema = get_table_schema(source_conn, source_schema, source_table)
        create_table(dest_conn, dest_schema, dest_table, schema)

    offset = 0
    rows_copied = 0

    with source_conn.cursor() as source_cursor, dest_conn.cursor() as dest_cursor:
        if copy_type == "full" or full_refresh:
            # Copia completa: Extraer en bloques de batch_size filas
            while True:
                source_cursor.execute(sql.SQL("SELECT * FROM {}.{} LIMIT %s OFFSET %s").format(
                    sql.Identifier(source_schema),
                    sql.Identifier(source_table)
                ), [batch_size, offset])
                
                rows = source_cursor.fetchall()

                if not rows:
                    break  # Si no hay más filas, salir del bucle

                columns = [desc[0] for desc in source_cursor.description]
                insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                    sql.Identifier(dest_schema),
                    sql.Identifier(dest_table),
                    sql.SQL(', ').join(map(sql.Identifier, columns)),
                    sql.SQL(', ').join(sql.Placeholder() * len(columns))
                )
                dest_cursor.executemany(insert_query, rows)
                dest_conn.commit()

                offset += batch_size
                rows_copied += len(rows)
                print(f"Copiadas {rows_copied} filas de la tabla {source_schema}.{source_table}")

        elif copy_type == "incremental" and incremental_column:
            # Copia incremental: Extraer solo las filas nuevas basadas en incremental_column
            dest_cursor.execute(sql.SQL("SELECT MAX({}) FROM {}.{}").format(
                sql.Identifier(incremental_column),
                sql.Identifier(dest_schema),
                sql.Identifier(dest_table)
            ))
            last_value = dest_cursor.fetchone()[0] or '1970-01-01'  # Por si está vacío
            
            while True:
                source_cursor.execute(sql.SQL("SELECT * FROM {}.{} WHERE {} > %s ORDER BY {} LIMIT %s OFFSET %s").format(
                    sql.Identifier(source_schema),
                    sql.Identifier(source_table),
                    sql.Identifier(incremental_column),
                    sql.Identifier(incremental_column)
                ), [last_value, batch_size, offset])

                rows = source_cursor.fetchall()

                if not rows:
                    break  # Si no hay más filas, salir del bucle

                columns = [desc[0] for desc in source_cursor.description]
                insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                    sql.Identifier(dest_schema),
                    sql.Identifier(dest_table),
                    sql.SQL(', ').join(map(sql.Identifier, columns)),
                    sql.SQL(', ').join(sql.Placeholder() * len(columns))
                )
                dest_cursor.executemany(insert_query, rows)
                dest_conn.commit()

                offset += batch_size
                rows_copied += len(rows)
                print(f"Copiadas {rows_copied} filas de la tabla {source_schema}.{source_table}")

        else:
            raise ValueError(f"copy_type '{copy_type}' no soportado o incremental_column no proporcionado.")

    print(f"Copia completada: {rows_copied} filas copiadas de {source_schema}.{source_table} a {dest_schema}.{dest_table}")


# Función principal
def main():
    # Configurar el parser de argumentos
    parser = argparse.ArgumentParser(description="Script para copiar tablas entre bases de datos PostgreSQL.")
    parser.add_argument('--full_refresh', action='store_true', help='Forzar carga completa para todas las tablas, incluso las incrementales.')

    # Parsear los argumentos
    args = parser.parse_args()

    # Obtener la ruta completa del archivo 'config.yaml' en la misma carpeta que el script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(current_dir, 'config.yml')

    # Cargar configuración
    config = load_config(config_file)

    for copy_info in config['copies']:
        # Obtener credenciales de las variables de entorno
        source_password = os.getenv(copy_info['source_pass_var'])
        dest_password = os.getenv(copy_info['dest_pass_var'])

        # Conectar a las bases de datos origen y destino
        source_conn = connect_to_db(
            host=copy_info['source_host'],
            port=copy_info['source_port'],
            user=copy_info['source_user'],
            password=source_password
        )
        dest_conn = connect_to_db(
            host=copy_info['dest_host'],  # Modificar si las DBs son diferentes
            port=copy_info['dest_port'],  # Modificar si las DBs son diferentes
            user=copy_info['dest_user'],  # Modificar si las DBs son diferentes
            password=dest_password
        )
    
        for table_info in copy_info['tables']:
            

            # Copiar la tabla según el tipo de copia y el flag de full_refresh
            copy_table(
                source_conn=source_conn,
                dest_conn=dest_conn,
                source_schema=table_info['source_schema'],
                source_table=table_info['source_table'],
                dest_schema=table_info['dest_schema'],
                dest_table=table_info['dest_table'],
                copy_type=table_info['copy_type'],
                incremental_column=table_info.get('incremental_column'),
                full_refresh=args.full_refresh  # Pasar el flag para forzar la carga completa
            )

        # Cerrar las conexiones
        source_conn.close()
        dest_conn.close()

if __name__ == "__main__":
    main()
