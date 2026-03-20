import pandas as pd
from conexiones import conectar_oltp, conectar_dw

def etl_dim_cliente():
    try:
        conn = conectar_oltp()

        query = """
        SELECT 
            c.CustomerID,
            UPPER(p.FirstName + ' ' + p.LastName) AS NombreCliente
        FROM Sales.Customer c
        JOIN Person.Person p 
            ON c.PersonID = p.BusinessEntityID
        """

        df = pd.read_sql(query, conn)
        conn.close()

        conn_dw = conectar_dw()
        cursor = conn_dw.cursor()

        for _, row in df.iterrows():

            cursor.execute("""
                SELECT COUNT(*) 
                FROM dbo.Dim_Cliente 
                WHERE CustomerKey = ?
            """, row.CustomerID)

            existe = cursor.fetchone()[0]

            if existe == 0:
                cursor.execute("""
                    INSERT INTO dbo.Dim_Cliente (CustomerKey, NombreCliente)
                    VALUES (?, ?)
                """,
                row.CustomerID,
                row.NombreCliente)

        conn_dw.commit()
        conn_dw.close()

        print("DimCliente cargada correctamente")

    except Exception as e:
        print("Error en DimCliente:", e)