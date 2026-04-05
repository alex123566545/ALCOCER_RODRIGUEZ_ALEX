import os
import sys
import pandas as pd
import gdown
import datetime

sys.path.append(os.path.abspath("../.secrets"))
from db_config import get_connection


# =============================
# LOG TXT
# =============================

log_file = "etl.log"

def escribir_log(msg):

    with open(log_file, "a", encoding="utf-8") as f:

        f.write(
            f"{datetime.datetime.now()} - {msg}\n"
        )


# =============================
# LOG EN META
# =============================

def log_db(conn, pipeline, msg):

    try:

        cursor = conn.cursor()

        cursor.execute(
            """
            insert into meta.pipeline_log
            (pipeline_name, mensaje)
            values (%s,%s)
            """,
            (pipeline, msg)
        )

        conn.commit()

    except Exception as e:

        print("Error log_db:", e)



# =============================
# MAIN
# =============================

def main():

    pipeline_name = "ETL1_BRONZE"

    escribir_log("Inicio ETL1")

    conn = get_connection()
    cursor = conn.cursor()

    try:

        # =============================
        # REGISTRAR INICIO
        # =============================

        cursor.execute(
            """
            insert into meta.pipeline_run
            (pipeline_name, fecha_inicio, estado)
            values (%s, now(), %s)
            returning id
            """,
            (pipeline_name, "RUNNING")
        )

        run_id = cursor.fetchone()[0]

        conn.commit()

        log_db(conn, pipeline_name, "Inicio pipeline")


        # =============================
        # LEER CONFIG
        # =============================

        cursor.execute(
            """
            select load_type
            from meta.pipeline_config
            where source_schema='bronze'
            and active=true
            limit 1
            """
        )

        row = cursor.fetchone()

        if row is None:

            raise Exception("No hay config en meta.pipeline_config")

        load_type = row[0]


        # =============================
        # DESCARGAR DRIVE
        # =============================

        file_id = "1Sk1E7GRJrOfEFh_286utp5jQKmAQBRTV"

        url = f"https://drive.google.com/uc?id={file_id}"

        log_db(conn, pipeline_name, "Descargando archivo")

        for f in os.listdir():

            if f.endswith(".part"):

                try:
                    os.remove(f)
                except:
                    pass


        archivo_local = gdown.download(
            url,
            quiet=False,
            fuzzy=True,
            resume=False
        )

        if archivo_local is None:

            raise Exception("No se descargo archivo")


        # =============================
        # LEER CSV
        # =============================

        df = pd.read_csv(archivo_local)

        df.columns = df.columns.str.strip()

        filas = len(df)

        print("Filas:", filas)

        log_db(conn, pipeline_name, f"Filas {filas}")


        # =============================
        # TRUNCATE SI FULL
        # =============================

        if load_type == "H":

            cursor.execute(
                "truncate table bronze.ventas_bronze"
            )

            conn.commit()

            log_db(conn, pipeline_name, "TRUNCATE bronze")


        # =============================
        # COPY
        # =============================

        temp_csv = archivo_local

        df.to_csv(temp_csv, index=False)

        with open(temp_csv, "r", encoding="utf-8") as f:

            cursor.copy_expert(
                """
                COPY bronze.ventas_bronze
                FROM STDIN
                WITH CSV HEADER
                """,
                f
            )

        conn.commit()

        log_db(conn, pipeline_name, "COPY OK")


        # =============================
        # FIN OK
        # =============================

        cursor.execute(
            """
            update meta.pipeline_run
            set fecha_fin = now(),
                estado = %s,
                filas_procesadas = %s
            where id = %s
            """,
            ("OK", filas, run_id)
        )

        conn.commit()

        log_db(conn, pipeline_name, "FIN OK")

        print("ETL1 terminado")


    except Exception as e:

        conn.rollback()

        print("ERROR:", e)

        log_db(conn, pipeline_name, f"ERROR {e}")

        cursor.execute(
            """
            update meta.pipeline_run
            set fecha_fin = now(),
                estado = 'ERROR'
            where id = %s
            """,
            (run_id,)
        )

        conn.commit()


    finally:

        cursor.close()
        conn.close()



# =============================
# EJECUCION
# =============================

if __name__ == "__main__":

    main()