from etl.transform import (
    process_dim_channel,
    process_dim_product,
    process_dim_customer,
    process_dim_province,
    process_dim_location,
    process_dim_store,
    process_dim_date,
    process_fact_sales_order,
    process_fact_sales_order_item,
    process_fact_web_session,
    process_fact_nps_response,
    process_fact_payment,
    process_fact_shipment,
)
from etl.load import RUTA_DW


if __name__ == "__main__":

    print("--- INICIANDO PROCESO DE TRANSFORMACIÓN (ETL) ---")

    print("\n--- PROCESANDO DIMENSIONES ---")
    process_dim_channel()
    process_dim_product()
    process_dim_customer()
    process_dim_province()
    process_dim_location()
    process_dim_store()
    df_date = process_dim_date()

    print("\n--- PROCESANDO TABLAS DE HECHOS ---")
    process_fact_sales_order(df_date)
    process_fact_sales_order_item(df_date)
    process_fact_web_session()
    process_fact_nps_response()
    process_fact_payment()
    process_fact_shipment()

    print("\n*** ¡Todas las transformaciones han sido completadas! ***")
    print(f"Todos los archivos CSV se han guardado en la carpeta: {RUTA_DW}")
