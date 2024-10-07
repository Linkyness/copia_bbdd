# PostgreSQL Table Copy Script

Este proyecto es un script de Python que permite copiar tablas de una base de datos PostgreSQL a otra, ya sea de forma completa o incremental. Además, puede manejar tablas muy grandes copiando los datos en bloques de 10,000 filas para evitar problemas de memoria.

## Características

- **Copia completa**: Transfiere todas las filas de la tabla de origen a la tabla de destino.
- **Copia incremental**: Transfiere solo las filas nuevas basadas en una columna incremental.
- **Copia en bloques**: Maneja grandes volúmenes de datos copiando las filas en bloques de 10,000 (o el tamaño que prefieras).
- **Creación automática de tablas**: Si la tabla de destino no existe, el script crea la tabla en la base de datos de destino utilizando el esquema de la tabla de origen.
- **Modo `full_refresh`**: Si se ejecuta con el parámetro `--full_refresh`, todas las tablas se copian completamente, incluso las que están configuradas para la copia incremental.

## Requisitos

- Python 3.7 o superior
- PostgreSQL
- Librerías de Python necesarias:
  - `psycopg2`
  - `pyyaml`
  
Puedes instalarlas con el siguiente comando:

```bash
pip install psycopg2-binary pyyaml
```

Las contraseñas de las bbdd se tomarán de variables de entrno, definidas como las siguientes:

```bash
export SOURCE_DB_PASSWORD=<password_origen>
export DEST_DB_PASSWORD=<password_destino>
```

## Ejecución
Se ejecutará utilizando uno de los siguientes comandos:

```bash
python3 copy_postgres_tables.py
python3 copy_postgres_tables.py --full_refresh
```

## Configuración
La configuración de las bbdd y tablas a copiar se encuentra dentro del fichero **config.yml**, que se deberá desplegar en la misma carpeta en la que se encuentre **copy_postgres_tables.py**. Este fichero se puede modificar a placer, siempre con las siguientes restricciones:
* Cada tabla deberá ser de tipo **full** o **incremental**
* En caso de ser **incremental**, deberá incluír su **incremental_column**
