import pandas as pd

from .extract import load_raw_data
from .load import save_to_dw


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


def process_dim_channel():
    print("\nProcesando: dim_channel...")
    df = load_raw_data('channel')
    if df is None:
        return
    df = df.rename(columns={'channel_id': 'id_channel',
                            'name': 'channel_name',
                            'code': 'channel_code'})
    save_to_dw(df, 'dim_channel')


def process_dim_product():
    print("\nProcesando: dim_product...")
    df_producto = load_raw_data('product')
    df_categoria = load_raw_data('product_category')
    if df_producto is None or df_categoria is None:
        return

    df_prod_full = pd.merge(df_producto, df_categoria,
                            on='category_id', how='left')
    df_prod_full = df_prod_full.rename(columns={
        'product_id': 'id_product',
        'name_x': 'product_name',
        'list_price': 'list_price',
        'name_y': 'category_name'
    })
    df_prod_final = df_prod_full[['id_product', 'sku', 'product_name',
                                  'list_price', 'category_name']]
    save_to_dw(df_prod_final, 'dim_product')


def process_dim_customer():
    print("\nProcesando: dim_customer...")
    df = load_raw_data('customer')
    if df is None:
        return

    df['full_name'] = df['first_name'].fillna('') + ' ' + df['last_name'].fillna('')
    df = df.rename(columns={
        'customer_id': 'id_customer',
        'status': 'status',
        'created_at': 'created_at'
    })
    df_final = df[['id_customer', 'email', 'full_name',
                   'status', 'created_at']]
    save_to_dw(df_final, 'dim_customer')


def process_dim_province():
    print("\nProcesando: dim_province...")
    df = load_raw_data('province')
    if df is None:
        return
    df = df.rename(columns={'province_id': 'id_province',
                            'name': 'province_name',
                            'code': 'province_code'})
    save_to_dw(df, 'dim_province')


def process_dim_location():
    print("\nProcesando: dim_location...")
    df = load_raw_data('address')
    if df is None:
        return
    df = df.rename(columns={'address_id': 'id_location',
                            'line1': 'address_line1',
                            'city': 'city',
                            'province_id': 'id_province'})
    df_final = df[['id_location', 'address_line1', 'city',
                   'id_province', 'postal_code']]
    save_to_dw(df_final, 'dim_location')


def process_dim_store():
    print("\nProcesando: dim_store...")
    df = load_raw_data('store')
    if df is None:
        return
    df = df.rename(columns={'store_id': 'id_store',
                            'name': 'store_name',
                            'address_id': 'id_location'})
    df_final = df[['id_store', 'store_name', 'id_location']]
    save_to_dw(df_final, 'dim_store')


def process_dim_date():
    print("\nProcesando: dim_date...")
    df_pedidos = load_raw_data('sales_order')
    if df_pedidos is None:
        return

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

    df_date_final = df_date[['id_date', 'full_date', 'year', 'month',
                             'month_name', 'day', 'quarter', 'day_of_week']]
    save_to_dw(df_date_final, 'dim_date')
    return df_date_final  

def process_fact_sales_order(df_date):
    print("\nProcesando: fact_sales_order...")
    df = load_raw_data('sales_order')
    if df is None or df_date is None:
        return

    estados_validos = ['PAID', 'FULFILLED']
    df = df[df['status'].isin(estados_validos)]

    df['order_date'] = parse_dates(df['order_date'])
    df['date_join'] = df['order_date'].dt.date
    df_date = df_date.copy()
    df_date['date_join'] = parse_dates(df_date['full_date']).dt.date

    df_fact = pd.merge(
        df,
        df_date[['id_date', 'date_join']],
        on='date_join',
        how='left'
    )

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
        'id_shipping_location', 'total_amount', 'subtotal',
        'tax_amount', 'shipping_fee'
    ]]
    save_to_dw(df_final, 'fact_sales_order')


def process_fact_sales_order_item(df_date):
    print("\nProcesando: fact_sales_order_item...")
    df_pedidos = load_raw_data('sales_order')
    df_items = load_raw_data('sales_order_item')
    if df_pedidos is None or df_items is None or df_date is None:
        return

    df_fact = pd.merge(df_items, df_pedidos, on='order_id', how='inner')

    estados_validos = ['PAID', 'FULFILLED']
    df_fact = df_fact[df_fact['status'].isin(estados_validos)]

    df_fact['order_date'] = parse_dates(df_fact['order_date'])
    df_fact['date_join'] = df_fact['order_date'].dt.date
    df_date = df_date.copy()
    df_date['date_join'] = parse_dates(df_date['full_date']).dt.date

    df_fact = pd.merge(
        df_fact,
        df_date[['id_date', 'date_join']],
        on='date_join',
        how='left'
    )

    df_fact = df_fact.rename(columns={
        'order_item_id': 'id_order_item',
        'order_id': 'id_order',
        'product_id': 'id_product',
        'customer_id': 'id_customer',
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
    if df is None:
        return

    df['started_at'] = parse_dates(df['started_at'])
    df = df.dropna(subset=['started_at'])
    df['id_date'] = convert_to_yyyymmdd(df['started_at'])

    df = df.rename(columns={'customer_id': 'id_customer'})

    df_final = df[['session_id', 'id_customer', 'id_date',
                   'source', 'device', 'started_at']]
    save_to_dw(df_final, 'fact_web_session')


def process_fact_nps_response():
    print("\nProcesando: fact_nps_response...")
    df = load_raw_data('nps_response')
    if df is None:
        return

    df['responded_at'] = parse_dates(df['responded_at'])
    df = df.dropna(subset=['responded_at'])
    df['id_date'] = convert_to_yyyymmdd(df['responded_at'])

    df = df.rename(columns={'customer_id': 'id_customer',
                            'channel_id': 'id_channel'})

    df_final = df[['nps_id', 'id_customer', 'id_channel',
                   'id_date', 'score', 'responded_at']]
    save_to_dw(df_final, 'fact_nps_response')


def process_fact_payment():
    print("\nProcesando: fact_payment...")
    df = load_raw_data('payment')
    if df is None:
        return

    df['paid_at'] = parse_dates(df['paid_at'])
    df = df.dropna(subset=['paid_at'])
    df['id_date'] = convert_to_yyyymmdd(df['paid_at'])

    df = df.rename(columns={'order_id': 'id_order',
                            'method': 'payment_method'})

    df_final = df[['payment_id', 'id_order', 'id_date',
                   'payment_method', 'status', 'amount', 'paid_at']]
    save_to_dw(df_final, 'fact_payment')


def process_fact_shipment():
    print("\nProcesando: fact_shipment...")
    df = load_raw_data('shipment')
    if df is None:
        return

    df['shipped_at'] = parse_dates(df['shipped_at'])
    df['delivered_at'] = parse_dates(df['delivered_at'])

    df['id_date_shipped'] = df['shipped_at'].apply(
        lambda x: convert_to_yyyymmdd(pd.Series([x]))[0] if pd.notna(x) else None
    )
    df['id_date_delivered'] = df['delivered_at'].apply(
        lambda x: convert_to_yyyymmdd(pd.Series([x]))[0] if pd.notna(x) else None
    )

    df = df.rename(columns={'order_id': 'id_order'})

    df_final = df[[
        'shipment_id', 'id_order', 'carrier', 'status', 'tracking_number',
        'id_date_shipped', 'shipped_at',
        'id_date_delivered', 'delivered_at'
    ]]
    save_to_dw(df_final, 'fact_shipment')
