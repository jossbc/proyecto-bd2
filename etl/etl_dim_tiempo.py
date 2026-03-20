import pandas as pd
from conexiones import conectar_oltp, conectar_dw

def etl_dim_tiempo():
    try:
        conn = conectar_oltp()

        query = """
        SELECT DISTINCT OrderDate
        FROM Sales.SalesOrderHeader
        """

        df = pd.read_sql(query, conn)
        conn.close()

        df["Fecha"] = pd.to_datetime(df["OrderDate"])
        df["TimeKey"] = df["Fecha"].dt.strftime('%Y%m%d').astype(int)
        df["Año"] = df["Fecha"].dt.year
        df["Mes"] = df["Fecha"].dt.month
        df["NombreMes"] = df["Fecha"].dt.month_name()
        df["Trimestre"] = df["Fecha"].dt.quarter
        df["Dia"] = df["Fecha"].dt.day

        conn_dw = conectar_dw()
        cursor = conn_dw.cursor()

        for _, row in df.iterrows():

            cursor.execute("""
                SELECT COUNT(*) 
                FROM dbo.Dim_Tiempo 
                WHERE TimeKey = ?
            """, row.TimeKey)

            existe = cursor.fetchone()[0]

            if existe == 0:
                cursor.execute("""
                    INSERT INTO dbo.Dim_Tiempo 
                    (TimeKey, Fecha, Año, Mes, NombreMes, Trimestre, Dia)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                row.TimeKey,
                row.Fecha,
                row.Año,
                row.Mes,
                row.NombreMes,
                row.Trimestre,
                row.Dia)

        conn_dw.commit()
        conn_dw.close()

        print("DimTiempo cargada correctamente")

    except Exception as e:
        print("Error en DimTiempo:", e)