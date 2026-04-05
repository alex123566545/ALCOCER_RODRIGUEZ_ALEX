import os
import sys
import pandas as pd

sys.path.append(os.path.abspath("../.secrets"))
from db_config import get_connection


def main():

    conn = get_connection()

    query = """
    select *
    from bronze.ventas_bronze
    """

    df = pd.read_sql(query, conn)

    df["mes"] = pd.to_datetime(df["fecha"]).dt.month
    df["dia_semana"] = pd.to_datetime(df["fecha"]).dt.dayofweek

    cursor = conn.cursor()

    cursor.execute("truncate table silver.ventas_silver")

    temp = "silver.csv"

    df.to_csv(temp, index=False)

    with open(temp, "r", encoding="utf-8") as f:
        cursor.copy_expert(
            """
            COPY silver.ventas_silver
            FROM STDIN
            WITH CSV HEADER
            """,
            f
        )

    conn.commit()

    print("Silver OK")


if __name__ == "__main__":
    main()