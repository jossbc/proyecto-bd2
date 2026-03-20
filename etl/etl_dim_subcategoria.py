import pandas as pd
from conexiones import conectar_oltp, conectar_dw

def etl_dim_subcategoria():
    try:
        conn = conectar_oltp()

        query = """
        SELECT 
            ps.ProductSubcategoryID,
            UPPER(ps.Name) AS NombreSubcategoria,
            ps.ProductCategoryID
        FROM Production.ProductSubcategory ps
        """

        df = pd.read_sql(query, conn)
        conn.close()

        conn_dw = conectar_dw()
        cursor = conn_dw.cursor()

        for _, row in df.iterrows():

            cursor.execute("""
                SELECT COUNT(*) 
                FROM dbo.Dim_Subcategoria 
                WHERE SubcategoriaKey = ?
            """, row.ProductSubcategoryID)

            existe = cursor.fetchone()[0]

            if existe == 0:
                cursor.execute("""
                    INSERT INTO dbo.Dim_Subcategoria 
                    (SubcategoriaKey, NombreSubcategoria, CategoriaKey)
                    VALUES (?, ?, ?)
                """,
                row.ProductSubcategoryID,
                row.NombreSubcategoria,
                row.ProductCategoryID)

        conn_dw.commit()
        conn_dw.close()

        print("DimSubcategoria cargada")

    except Exception as e:
        print("Error en DimSubcategoria:", e)