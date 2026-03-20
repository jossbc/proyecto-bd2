import pandas as pd
from conexiones import conectar_oltp, conectar_dw

def etl_dim_producto():
    try:
        conn = conectar_oltp()

        query = """
        SELECT 
            p.ProductID,
            UPPER(p.Name) AS NombreProducto,
            ps.ProductSubcategoryID,
            ps.Name AS NombreSubcategoria,
            pc.ProductCategoryID,
            pc.Name AS NombreCategoria
        FROM Production.Product p
        LEFT JOIN Production.ProductSubcategory ps 
            ON p.ProductSubcategoryID = ps.ProductSubcategoryID
        LEFT JOIN Production.ProductCategory pc 
            ON ps.ProductCategoryID = pc.ProductCategoryID
        """

        df = pd.read_sql(query, conn)
        conn.close()

        conn_dw = conectar_dw()
        cursor = conn_dw.cursor()

        for _, row in df.iterrows():

            cursor.execute("""
                SELECT COUNT(*) 
                FROM DimProducto 
                WHERE ProductKey = ?
            """, row.ProductID)

            existe = cursor.fetchone()[0]

            if existe == 0:
                cursor.execute("""
                    INSERT INTO dbo.Dim_Producto 
                    (ProductKey, NombreProducto, SubcategoriaKey)
                    VALUES (?, ?, ?)
                """,
                row.ProductID,
                row.NombreProducto,
                row.ProductSubcategoryID)

        conn_dw.commit()
        conn_dw.close()

        print("DimProducto cargada correctamente")

    except Exception as e:
        print("Error en ETL DimProducto:", e)