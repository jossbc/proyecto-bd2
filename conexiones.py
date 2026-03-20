import pyodbc

def conectar_oltp():
    try:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=localhost;"
            "DATABASE=AdventureWorks2025;"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )
        print("Conectado a OLTP")
        return conn
    except Exception as e:
        print("Error conexión OLTP:", e)

def conectar_dw():
    try:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=localhost;"
            "DATABASE=DW_AdventureWorks2025;"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )
        print("Conectado a DW")
        return conn
    except Exception as e:
        print("Error conexión DW:", e)


if __name__ == "__main__":
    conn1 = conectar_oltp()
    conn2 = conectar_dw()

    if conn1:
        conn1.close()
    if conn2:
        conn2.close()