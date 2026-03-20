import pandas as pd
from conexiones import conectar_oltp, conectar_dw

def etl_dim_territorio():
    try:
        conn = conectar_oltp()

        query = """
        SELECT 
            TerritoryID,
            UPPER(Name) AS NombreTerritorio,
            CountryRegionCode,
            [Group] AS Grupo
        FROM Sales.SalesTerritory
        """

        df = pd.read_sql(query, conn)
        conn.close()

        df["Region"] = df["CountryRegionCode"]
        df["Pais"] = df["CountryRegionCode"]

        conn_dw = conectar_dw()
        cursor = conn_dw.cursor()

        for _, row in df.iterrows():

            cursor.execute("""
                SELECT COUNT(*) 
                FROM DimTerritorio 
                WHERE TerritoryKey = ?
            """, row.TerritoryID)

            existe = cursor.fetchone()[0]

            if existe == 0:
                cursor.execute("""
                    INSERT INTO DimTerritorio 
                    (TerritoryKey, NombreTerritorio, Region, Pais, Grupo)
                    VALUES (?, ?, ?, ?, ?)
                """,
                row.TerritoryID,
                row.NombreTerritorio,
                row.Region,
                row.Pais,
                row.Grupo)

        conn_dw.commit()
        conn_dw.close()

        print("DimTerritorio cargada correctamente")

    except Exception as e:
        print("Error en DimTerritorio:", e)