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

        print("Cargando DimCategoria...")
        etl_dim_categoria()

        print("Cargando DimSubcategoria...")
        etl_dim_subcategoria()

        print("Cargando DimProducto...")
        etl_dim_producto()

        print("Cargando DimCliente...")
        etl_dim_cliente()

        print("Cargando DimTerritorio...")
        etl_dim_territorio()

        print("Cargando DimTiempo...")
        etl_dim_tiempo()

        print("Cargando FactVentas...")
        etl_hechos_ventas()

        print("\n🎉 ETL COMPLETADO CORRECTAMENTE")

    except Exception as e:
        print("\n❌ ERROR EN EL PROCESO ETL:")
        print(e)


if __name__ == "__main__":
    ejecutar_etl_completo()