import os
import sys
import pandas as pd
import datetime
from io import StringIO
import pickle

from sklearn.ensemble import RandomForestRegressor

sys.path.append(os.path.abspath("../.secrets"))
from db_config import get_connection


# =============================
# LOG TXT
# =============================

log_file = "etl.log"

def escribir_log(msg):
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"{datetime.datetime.now()} - {msg}\n")


# =============================
# LOG EN BD
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

    pipeline_name = "ETL3_GOLD"
    run_id = None

    escribir_log("Inicio ETL3 GOLD")

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

        result = cursor.fetchone()

        if result is None:
            raise Exception("No se pudo crear pipeline_run")

        run_id = result[0]
        conn.commit()

        log_db(conn, pipeline_name, f"Inicio pipeline ID={run_id}")


        # =============================
        # LEER DATA
        # =============================

        df = pd.read_sql("select * from gold_ml.ventas_dataset", conn)
        df_new = pd.read_sql("select * from gold_ml.target_nuevos", conn)

        log_db(conn, pipeline_name, f"Dataset entrenamiento: {len(df)}")
        log_db(conn, pipeline_name, f"Dataset nuevos: {len(df_new)}")


        # =============================
        # PREPARAR DATOS
        # =============================

        y = df["cantidad_vendida"]

        X = df.drop("cantidad_vendida", axis=1)
        X = pd.get_dummies(X)


        # =============================
        # MODELO
        # =============================

        model = RandomForestRegressor(
            n_estimators=20,
            max_depth=5,
            random_state=42,
            n_jobs=-1
        )

        model.fit(X, y)


        # =============================
        # GUARDAR MODELO PKL
        # =============================

        with open("modelo_ventas.pkl", "wb") as f:
            pickle.dump(model, f)

        with open("columnas_modelo.pkl", "wb") as f:
            pickle.dump(X.columns.tolist(), f)

        log_db(conn, pipeline_name, "Modelo entrenado y guardado en PKL")


        # =============================
        # PREDICCIÓN
        # =============================

        X_new = pd.get_dummies(df_new)
        X_new = X_new.reindex(columns=X.columns, fill_value=0)

        pred = model.predict(X_new)

        df_new["prediccion"] = pred

        log_db(conn, pipeline_name, "Prediccion generada")


        # =============================
        # LIMPIAR TABLA
        # =============================

        cursor.execute("truncate table gold_ml.resultado_predicciones")
        conn.commit()

        log_db(conn, pipeline_name, "TRUNCATE resultado")


        # =============================
        # INSERT MASIVO
        # =============================

        buffer = StringIO()
        df_new.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        cursor.copy_expert(
            """
            COPY gold_ml.resultado_predicciones
            FROM STDIN WITH CSV
            """,
            buffer
        )

        conn.commit()

        filas = len(df_new)

        log_db(conn, pipeline_name, f"Filas insertadas: {filas}")


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

        print("ETL3 GOLD terminado 🚀")


    except Exception as e:

        conn.rollback()

        print("ERROR:", e)

        log_db(conn, pipeline_name, f"ERROR {e}")

        if run_id is not None:

            cursor.execute(
                """
                update meta.pipeline_run
                set fecha_fin = now(),
                    estado = 'ERROR',
                    mensaje_error = %s
                where id = %s
                """,
                (str(e), run_id)
            )

            conn.commit()


    finally:

        cursor.close()
        conn.close()


# =============================
# EJECUCIÓN
# =============================

if __name__ == "__main__":
    main()