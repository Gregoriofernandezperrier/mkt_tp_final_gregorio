import pandas as pd
import os

# --- Configuración de Rutas Globales ---
RUTA_RAW = 'RAW'
RUTA_DW = 'DW'

# --- Funciones Auxiliares (Helpers) ---

def load_raw_data(table_name):
    """Carga un archivo CSV desde la carpeta RAW."""
    raw_path = os.path.join(RUTA_RAW, f'{table_name}.csv')
    try:
        df = pd.read_csv(raw_path)
        print(f"Datos cargados desde: {raw_path}")
        return df
    except FileNotFoundError:
        print(f"ERROR: Archivo no encontrado en {raw_path}")
        return None

def save_to_dw(df, table_name):
    """Guarda un DataFrame en la carpeta DW."""
    # Asegurarse de que el directorio DW exista
    os.makedirs(RUTA_DW, exist_ok=True)
    
    dw_path = os.path.join(RUTA_DW, f'{table_name}.csv')
    df.to_csv(dw_path, index=False)
    print(f"Datos guardados en: {dw_path}")

# --- Funciones de Conversión de Fecha ---

def convert_to_yyyymmdd(date_column):
    """Convierte una columna de datetime a entero YYYYMMDD."""
    return (
        date_column.dt.year * 10000 +
        date_column.dt.month * 100 +
        date_column.dt.day
    ).astype(int)

def parse_dates(column, errors='coerce'):
    """Convierte una columna a datetime de forma segura."""
    return pd.to_datetime(column, errors=errors)

# --- 1. Transformación de Dimensiones ---

def process_dim_channel():
    print("\nProcesando: dim_channel...")
    df = load_raw_data('channel')
    if df is None: return
    # Renombrar para coincidir con la imagen (aunque ya estaba cerca)
    df = df.rename(columns={'channel_id': 'id_channel', 'name': 'channel_name', 'code': 'channel_code'})
    save_to_dw(df, 'dim_channel')

def process_dim_product():
    print("\nProcesando: dim_product...")
    df_producto = load_raw_data('product')
    df_categoria = load_raw_data('product_category')
    if df_producto is None or df_categoria is None: return

    df_prod_full = pd.merge(df_producto, df_categoria, on='category_id', how='left')
    df_prod_full = df_prod_full.rename(columns={
        'product_id': 'id_product',
        'name_x': 'product_name',
        'list_price': 'list_price',
        'name_y': 'category_name'
    })
    df_prod_final = df_prod_full[['id_product', 'sku', 'product_name', 'list_price', 'category_name']]
    save_to_dw(df_prod_final, 'dim_product')

def process_dim_customer():
    print("\nProcesando: dim_customer...")
    df = load_raw_data('customer')
    if df is None: return

    df['full_name'] = df['first_name'].fillna('') + ' ' + df['last_name'].fillna('')
    df = df.rename(columns={
        'customer_id': 'id_customer',
        'status': 'status',
        'created_at': 'created_at'
    })
    df_final = df[['id_customer', 'email', 'full_name', 'status', 'created_at']]
    save_to_dw(df_final, 'dim_customer')

def process_dim_province():
    print("\nProcesando: dim_province...")
    df = load_raw_data('province')
    if df is None: return
    df = df.rename(columns={'province_id': 'id_province', 'name': 'province_name', 'code': 'province_code'})
    save_to_dw(df, 'dim_province')

def process_dim_location():
    print("\nProcesando: dim_location...")
    df = load_raw_data('address')
    if df is None: return
    # Esta tabla es la 'dim_location' de la imagen
    df = df.rename(columns={'address_id': 'id_location', 'line1': 'address_line1', 'city': 'city', 'province_id': 'id_province'})
    df_final = df[['id_location', 'address_line1', 'city', 'id_province', 'postal_code']]
    save_to_dw(df_final, 'dim_location')
    
def process_dim_store():
    print("\nProcesando: dim_store...")
    df = load_raw_data('store')
    if df is None: return
    # Esta era tu 'dim_geografia'
    df = df.rename(columns={'store_id': 'id_store', 'name': 'store_name', 'address_id': 'id_location'})
    df_final = df[['id_store', 'store_name', 'id_location']]
    save_to_dw(df_final, 'dim_store')

def process_dim_date():
    print("\nProcesando: dim_date...")
    df_pedidos = load_raw_data('sales_order')
    if df_pedidos is None: return

    df_pedidos['order_date'] = parse_dates(df_pedidos['order_date'])
    fecha_min = df_pedidos['order_date'].min()
    fecha_max = df_pedidos['order_date'].max()
    print(f"Rango de fechas detectado: {fecha_min} a {fecha_max}")

    date_range = pd.date_range(start=fecha_min, end=fecha_max, freq='D')
    df_date = pd.DataFrame(date_range, columns=['full_date'])

    df_date['id_date'] = df_date['full_date'].dt.strftime('%Y%m%d').astype(int)
    df_date['year'] = df_date['full_date'].dt.year
    df_date['month'] = df_date['full_date'].dt.month
    df_date['month_name'] = df_date['full_date'].dt.strftime('%B')
    df_date['day'] = df_date['full_date'].dt.day
    df_date['quarter'] = df_date['full_date'].dt.quarter
    df_date['day_of_week'] = df_date['full_date'].dt.dayofweek

    df_date_final = df_date[['id_date', 'full_date', 'year', 'month', 'month_name', 'day', 'quarter', 'day_of_week']]
    save_to_dw(df_date_final, 'dim_date')
    return df_date_final # Para usar en las facts

# --- 2. Transformación de Tablas de Hechos ---

def process_fact_sales_order(df_date):
    print("\nProcesando: fact_sales_order...")
    df = load_raw_data('sales_order')
    if df is None or df_date is None: return
    
    # Filtrar por ventas válidas [cite: 180]
    estados_validos = ['PAID', 'FULFILLED']
    df = df[df['status'].isin(estados_validos)]

    # Preparar fechas para el join
    df['order_date'] = parse_dates(df['order_date'])
    df['date_join'] = df['order_date'].dt.date
    df_date['date_join'] = parse_dates(df_date['full_date']).dt.date

    # Join con dim_date
    df_fact = pd.merge(
        df,
        df_date[['id_date', 'date_join']],
        on='date_join',
        how='left'
    )
    
    # Renombrar y seleccionar
    df_fact = df_fact.rename(columns={
        'order_id': 'id_order',
        'customer_id': 'id_customer',
        'channel_id': 'id_channel',
        'store_id': 'id_store',
        'billing_address_id': 'id_billing_location',
        'shipping_address_id': 'id_shipping_location',
        'total_amount': 'total_amount',
        'subtotal': 'subtotal',
        'tax_amount': 'tax_amount',
        'shipping_fee': 'shipping_fee'
    })
    
    df_final = df_fact[[
        'id_order', 'id_date', 'id_customer', 'id_channel', 'id_store',
        'id_shipping_location', 'total_amount', 'subtotal', 'tax_amount', 'shipping_fee'
    ]]
    save_to_dw(df_final, 'fact_sales_order')

def process_fact_sales_order_item(df_date):
    print("\nProcesando: fact_sales_order_item...")
    df_pedidos = load_raw_data('sales_order')
    df_items = load_raw_data('sales_order_item')
    if df_pedidos is None or df_items is None or df_date is None: return

    # Join para obtener fechas y FKs del pedido
    df_fact = pd.merge(df_items, df_pedidos, on='order_id', how='inner')

    # Filtrar por ventas válidas [cite: 180]
    estados_validos = ['PAID', 'FULFILLED']
    df_fact = df_fact[df_fact['status'].isin(estados_validos)]

    # Preparar fechas para el join
    df_fact['order_date'] = parse_dates(df_fact['order_date'])
    df_fact['date_join'] = df_fact['order_date'].dt.date
    df_date['date_join'] = parse_dates(df_date['full_date']).dt.date

    # Join con dim_date
    df_fact = pd.merge(
        df_fact,
        df_date[['id_date', 'date_join']],
        on='date_join',
        how='left'
    )
    
    # Renombrar y seleccionar
    df_fact = df_fact.rename(columns={
        'order_item_id': 'id_order_item',
        'order_id': 'id_order',
        'product_id': 'id_product',
        'customer_id': 'id_customer', # De la cabecera del pedido
        'quantity': 'quantity',
        'unit_price': 'unit_price',
        'discount_amount': 'discount_amount',
        'line_total': 'line_total'
    })
    
    df_final = df_fact[[
        'id_order_item', 'id_order', 'id_date', 'id_product', 'id_customer',
        'quantity', 'unit_price', 'discount_amount', 'line_total'
    ]]
    save_to_dw(df_final, 'fact_sales_order_item')

def process_fact_web_session():
    print("\nProcesando: fact_web_session...")
    df = load_raw_data('web_session')
    if df is None: return

    df['started_at'] = parse_dates(df['started_at'])
    df = df.dropna(subset=['started_at'])
    df['id_date'] = convert_to_yyyymmdd(df['started_at'])
    
    df = df.rename(columns={'customer_id': 'id_customer'})
    
    df_final = df[['session_id', 'id_customer', 'id_date', 'source', 'device', 'started_at']]
    save_to_dw(df_final, 'fact_web_session')

def process_fact_nps_response():
    print("\nProcesando: fact_nps_response...")
    df = load_raw_data('nps_response')
    if df is None: return

    df['responded_at'] = parse_dates(df['responded_at'])
    df = df.dropna(subset=['responded_at'])
    df['id_date'] = convert_to_yyyymmdd(df['responded_at'])

    df = df.rename(columns={'customer_id': 'id_customer', 'channel_id': 'id_channel'})

    df_final = df[['nps_id', 'id_customer', 'id_channel', 'id_date', 'score', 'responded_at']]
    save_to_dw(df_final, 'fact_nps_response')

def process_fact_payment():
    print("\nProcesando: fact_payment...")
    df = load_raw_data('payment')
    if df is None: return
    
    df['paid_at'] = parse_dates(df['paid_at'])
    # Solo nos interesan los pagos que tienen fecha
    df = df.dropna(subset=['paid_at'])
    df['id_date'] = convert_to_yyyymmdd(df['paid_at'])

    df = df.rename(columns={'order_id': 'id_order', 'method': 'payment_method'})
    
    df_final = df[['payment_id', 'id_order', 'id_date', 'payment_method', 'status', 'amount', 'paid_at']]
    save_to_dw(df_final, 'fact_payment')

def process_fact_shipment():
    print("\nProcesando: fact_shipment...")
    df = load_raw_data('shipment')
    if df is None: return

    # Esta tabla tiene múltiples fechas
    df['shipped_at'] = parse_dates(df['shipped_at'])
    df['delivered_at'] = parse_dates(df['delivered_at'])

    # Creamos un id_date para cada fecha
    df['id_date_shipped'] = df['shipped_at'].apply(lambda x: convert_to_yyyymmdd(pd.Series([x]))[0] if pd.notna(x) else None)
    df['id_date_delivered'] = df['delivered_at'].apply(lambda x: convert_to_yyyymmdd(pd.Series([x]))[0] if pd.notna(x) else None)
    
    df = df.rename(columns={'order_id': 'id_order'})
    
    df_final = df[[
        'shipment_id', 'id_order', 'carrier', 'status', 'tracking_number',
        'id_date_shipped', 'shipped_at', 
        'id_date_delivered', 'delivered_at'
    ]]
    save_to_dw(df_final, 'fact_shipment')


# --- PUNTO DE ENTRADA PRINCIPAL ---
if __name__ == "__main__":
    
    print("--- INICIANDO PROCESO DE TRANSFORMACIÓN (ETL) ---")
    
    # 1. Ejecutar Dimensiones
    print("\n--- PROCESANDO DIMENSIONES ---")
    process_dim_channel()
    process_dim_product()
    process_dim_customer()
    process_dim_province()
    process_dim_location()
    process_dim_store()
    # Guardamos la dim_date en una variable para pasarla a las facts
    df_date = process_dim_date()
    
    # 2. Ejecutar Tablas de Hechos
    print("\n--- PROCESANDO TABLAS DE HECHOS ---")
    process_fact_sales_order(df_date)
    process_fact_sales_order_item(df_date)
    process_fact_web_session()
    process_fact_nps_response()
    process_fact_payment()
    process_fact_shipment()
    
    print("\n*** ¡Todas las transformaciones han sido completadas! ***")
    print(f"Todos los archivos CSV se han guardado en la carpeta: {RUTA_DW}")