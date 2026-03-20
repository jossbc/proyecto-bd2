import pandas as pd
from conexiones import conectar_oltp, conectar_dw

def etl_hechos_ventas():
    try:
        conn = conectar_oltp()

        query = """
        SELECT 
            sod.ProductID,
            soh.CustomerID,
            soh.TerritoryID,
            soh.OrderDate,
            sod.OrderQty,
            sod.LineTotal,
            sod.UnitPriceDiscount
        FROM Sales.SalesOrderDetail sod
        JOIN Sales.SalesOrderHeader soh 
            ON sod.SalesOrderID = soh.SalesOrderID
        """

        df = pd.read_sql(query, conn)
        conn.close()

        # 🔄 TRANSFORMACIÓN
        df["TimeKey"] = pd.to_datetime(df["OrderDate"]).dt.strftime('%Y%m%d').astype(int)

        conn_dw = conectar_dw()
        cursor = conn_dw.cursor()

        for _, row in df.iterrows():

            # 🔍 VALIDAR DUPLICADOS (IMPORTANTE)
            cursor.execute("""
                SELECT COUNT(*) FROM dbo.Hechos_Ventas
                WHERE ProductKey=? AND CustomerKey=? AND TimeKey=?
            """,
            row.ProductID,
            row.CustomerID,
            row.TimeKey)

            existe = cursor.fetchone()[0]

            if existe == 0:
                cursor.execute("""
                    INSERT INTO dbo.Hechos_Ventas
                    (ProductKey, CustomerKey, TimeKey, TerritoryKey, OrderQty, SalesAmount, DiscountAmount)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                row.ProductID,
                row.CustomerID,
                row.TimeKey,
                row.TerritoryID,
                row.OrderQty,
                row.LineTotal,
                row.UnitPriceDiscount)

        conn_dw.commit()
        conn_dw.close()

        print("Hechos_Venta cargada correctamente")

    except Exception as e:
        print("Error en Hechos_Venta:", e)