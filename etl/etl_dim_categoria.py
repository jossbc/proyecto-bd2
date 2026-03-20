import pandas as pd
from conexiones import conectar_oltp, conectar_dw

def etl_dim_categoria():
    try:
        conn = conectar_oltp()

        query = """
        SELECT DISTINCT
            pc.ProductCategoryID,
            UPPER(pc.Name) AS NombreCategoria
        FROM Production.ProductCategory pc
        """

        df = pd.read_sql(query, conn)
        conn.close()

        conn_dw = conectar_dw()
        cursor = conn_dw.cursor()

        for _, row in df.iterrows():

            cursor.execute("""
                SELECT COUNT(*) 
                FROM DimCategoria 
                WHERE CategoriaKey = ?
            """, row.ProductCategoryID)

            existe = cursor.fetchone()[0]

            if existe == 0:
                cursor.execute("""
                    INSERT INTO dbo.Dim_Categoria (CategoriaKey, NombreCategoria)
                    VALUES (?, ?)
                """,
                row.ProductCategoryID,
                row.NombreCategoria)

        conn_dw.commit()
        conn_dw.close()

        print("DimCategoria cargada")

    except Exception as e:
        print("Error en DimCategoria:", e)