from etl.etl_dim_categoria import etl_dim_categoria
from etl.etl_dim_subcategoria import etl_dim_subcategoria
from etl.etl_dim_producto import etl_dim_producto
from etl.etl_dim_cliente import etl_dim_cliente
from etl.etl_dim_territorio import etl_dim_territorio
from etl.etl_dim_tiempo import etl_dim_tiempo
from etl.etl_hechos_ventas import etl_hechos_ventas

def ejecutar_etl_completo():
    try:
        print("\nINICIANDO PROCESO ETL\n")

        print("Cargando Dim_Categoria...")
        etl_dim_categoria()

        print("Cargando DimSubcategoria...")
        etl_dim_subcategoria()

        print("Cargando Dim_Producto...")
        etl_dim_producto()

        print("Cargando Dim_Cliente...")
        etl_dim_cliente()

        print("Cargando Dim_Territorio...")
        etl_dim_territorio()

        print("Cargando Dim_Tiempo...")
        etl_dim_tiempo()

        print("Cargando Hechos_Ventas...")
        etl_hechos_ventas()

        print("\nETL COMPLETADO CORRECTAMENTE")

    except Exception as e:
        print("\nERROR EN EL PROCESO ETL:")
        print(e)


if __name__ == "__main__":
    ejecutar_etl_completo()