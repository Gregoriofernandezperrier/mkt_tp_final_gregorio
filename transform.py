import pandas as pd
import os

# Definir las rutas
ruta_raw = 'RAW'
ruta_dw = 'DW'

# Asegurarse de que el directorio DW exista
os.makedirs(ruta_dw, exist_ok=True)

# --- 1. Transformación de dim_canal ---
print("Iniciando transformación de Canales...")

# Leer el CSV original
df_canal = pd.read_csv(os.path.join(ruta_raw, 'channel.csv'))

# Renombrar columnas (Modelado)
df_canal = df_canal.rename(columns={
    'channel_id': 'id_canal',
    'name': 'nombre_canal',
    'code': 'codigo_canal'  # Mantenemos el código por si acaso
})

# Seleccionar solo las columnas que queremos
df_canal_final = df_canal[['id_canal', 'nombre_canal', 'codigo_canal']]

# Guardar el archivo transformado en DW
ruta_salida = os.path.join(ruta_dw, 'dim_canal.csv')
df_canal_final.to_csv(ruta_salida, index=False)

print(f"Canales transformados y guardados en: {ruta_salida}")
print("--- Transformación de Canales Finalizada ---")

# --- 2. Transformación de dim_producto ---
print("\nIniciando transformación de Productos...")

# Leer los CSV originales
df_producto = pd.read_csv(os.path.join(ruta_raw, 'product.csv'))
df_categoria = pd.read_csv(os.path.join(ruta_raw, 'product_category.csv'))

# --- Transformación (Join) ---
# Unimos los dos dataframes para tener el nombre de la categoría
# Es como un VLOOKUP o un JOIN de SQL
df_prod_full = pd.merge(
    df_producto,
    df_categoria,
    on='category_id', # La clave que tienen en común
    how='left'        # 'left' para mantener todos los productos, tengan o no categoría
)

# Renombrar columnas (Modelado)
df_prod_full = df_prod_full.rename(columns={
    'product_id': 'id_producto',
    'sku': 'sku',
    'name_x': 'nombre_producto',   # 'name_x' es el 'name' de producto
    'list_price': 'precio_lista',
    'name_y': 'nombre_categoria' # 'name_y' es el 'name' de categoría
})

# Seleccionar solo las columnas que queremos
df_prod_final = df_prod_full[[
    'id_producto', 
    'sku', 
    'nombre_producto', 
    'precio_lista', 
    'nombre_categoria'
]]

# Guardar el archivo transformado en DW
ruta_salida = os.path.join(ruta_dw, 'dim_producto.csv')
df_prod_final.to_csv(ruta_salida, index=False)

print(f"Productos transformados y guardados en: {ruta_salida}")
print("--- Transformación de Productos Finalizada ---")


# --- 3. Transformación de dim_cliente ---
print("\nIniciando transformación de Clientes...")

# Leer el CSV original
df_cliente = pd.read_csv(os.path.join(ruta_raw, 'customer.csv'))

# --- Transformación ---
# Combinar nombre y apellido en una sola columna
# .fillna('') se usa por si algún nombre o apellido falta (NaN), para evitar errores
df_cliente['nombre_completo'] = df_cliente['first_name'].fillna('') + ' ' + df_cliente['last_name'].fillna('')

# Renombrar columnas (Modelado)
df_cliente = df_cliente.rename(columns={
    'customer_id': 'id_cliente',
    'email': 'email',
    'status': 'estado',
    'created_at': 'fecha_alta'
})

# Seleccionar solo las columnas que queremos
df_cliente_final = df_cliente[[
    'id_cliente', 
    'email', 
    'nombre_completo', 
    'estado',
    'fecha_alta'
]]

# Guardar el archivo transformado en DW
ruta_salida = os.path.join(ruta_dw, 'dim_cliente.csv')
df_cliente_final.to_csv(ruta_salida, index=False)

print(f"Clientes transformados y guardados en: {ruta_salida}")
print("--- Transformación de Clientes Finalizada ---")


# --- 4. Transformación de dim_geografia ---
print("\nIniciando transformación de Geografía...")

# Leer los CSV originales
df_provincia = pd.read_csv(os.path.join(ruta_raw, 'province.csv'))
df_direccion = pd.read_csv(os.path.join(ruta_raw, 'address.csv'))
df_tienda = pd.read_csv(os.path.join(ruta_raw, 'store.csv'))

# --- Transformación (Joins) ---
# 1. Unir Tiendas con Direcciones
df_geo = pd.merge(
    df_tienda,
    df_direccion,
    on='address_id',
    how='left' # Queremos todas las tiendas y su dirección
)

# 2. Unir el resultado con Provincias
df_geo = pd.merge(
    df_geo,
    df_provincia,
    on='province_id',
    how='left' # Queremos la provincia de esa dirección
)

# Renombrar columnas (Modelado)
# Usamos .rename con un diccionario grande
df_geo = df_geo.rename(columns={
    'store_id': 'id_tienda',
    'name_x': 'nombre_tienda',   # 'name_x' es el 'name' de store
    'address_id': 'id_direccion',
    'line1': 'direccion_linea1',
    'city': 'ciudad',
    'province_id': 'id_provincia',
    'name_y': 'nombre_provincia' # 'name_y' es el 'name' de province
})

# Seleccionar solo las columnas que queremos
# Dejamos fuera 'name' (de province) y otras que no sirven
df_geo_final = df_geo[[
    'id_tienda', 
    'nombre_tienda', 
    'id_direccion',
    'direccion_linea1',
    'ciudad',
    'id_provincia',
    'nombre_provincia'
]]

# Guardar el archivo transformado en DW
ruta_salida = os.path.join(ruta_dw, 'dim_geografia.csv')
df_geo_final.to_csv(ruta_salida, index=False)

print(f"Geografía (Tiendas) transformada y guardada en: {ruta_salida}")
print("--- Transformación de Geografía Finalizada ---")


# --- 5. Transformación de dim_tiempo ---
print("\nIniciando transformación de Tiempo...")

# Para crear la dimensión tiempo, primero necesitamos saber el rango de fechas
# Leeremos 'sales_order.csv' solo para sacar la fecha mínima y máxima
df_pedidos_para_fechas = pd.read_csv(os.path.join(ruta_raw, 'sales_order.csv'))

# Convertir 'order_date' a tipo datetime para poder operar
df_pedidos_para_fechas['order_date'] = pd.to_datetime(df_pedidos_para_fechas['order_date'])

# Encontrar la fecha mínima y máxima
fecha_min = df_pedidos_para_fechas['order_date'].min()
fecha_max = df_pedidos_para_fechas['order_date'].max()

print(f"Rango de fechas detectado: {fecha_min} a {fecha_max}")

# Crear un rango de fechas completo entre la mínima y la máxima
date_range = pd.date_range(start=fecha_min, end=fecha_max, freq='D')

# Crear el DataFrame de la dimensión tiempo
df_tiempo = pd.DataFrame(date_range, columns=['fecha_completa'])

# --- Transformación (Extracción de atributos) ---
df_tiempo['id_fecha'] = df_tiempo['fecha_completa'].dt.strftime('%Y%m%d').astype(int) # Clave YYYYMMDD
df_tiempo['anio'] = df_tiempo['fecha_completa'].dt.year
df_tiempo['mes'] = df_tiempo['fecha_completa'].dt.month
df_tiempo['nombre_mes'] = df_tiempo['fecha_completa'].dt.strftime('%B') # '%B' es el nombre completo
df_tiempo['dia'] = df_tiempo['fecha_completa'].dt.day
df_tiempo['trimestre'] = df_tiempo['fecha_completa'].dt.quarter
df_tiempo['dia_semana'] = df_tiempo['fecha_completa'].dt.dayofweek # Lunes=0, Domingo=6

# Seleccionar y ordenar las columnas
df_tiempo_final = df_tiempo[[
    'id_fecha',
    'fecha_completa',
    'anio',
    'mes',
    'nombre_mes',
    'dia',
    'trimestre',
    'dia_semana'
]]

# Guardar el archivo transformado en DW
ruta_salida = os.path.join(ruta_dw, 'dim_tiempo.csv')
df_tiempo_final.to_csv(ruta_salida, index=False)

print(f"Dimensión Tiempo creada y guardada en: {ruta_salida}")
print("--- Transformación de Tiempo Finalizada ---")


# --- 6. Creación de la fact_ventas ---
print("\nIniciando creación de la Tabla de Hechos: fact_ventas...")

# Leer los CSV transaccionales
df_pedidos = pd.read_csv(os.path.join(ruta_raw, 'sales_order.csv'))
df_items = pd.read_csv(os.path.join(ruta_raw, 'sales_order_item.csv'))

# Leer la dimensión de tiempo que creamos
df_tiempo_dim = pd.read_csv(os.path.join(ruta_dw, 'dim_tiempo.csv'))

# --- Transformación ---

# 1. Convertir fechas
# Mantenemos 'order_date' original (que es un timestamp)
# Creamos una *nueva* columna 'fecha_join' solo para unir con dim_tiempo
df_pedidos['fecha_join'] = pd.to_datetime(df_pedidos['order_date']).dt.date

# Convertir 'fecha_completa' de dim_tiempo a solo fecha
df_tiempo_dim['fecha_completa'] = pd.to_datetime(df_tiempo_dim['fecha_completa']).dt.date

# 2. Unir pedidos con items
df_ventas = pd.merge(
    df_pedidos,
    df_items,
    on='order_id',
    how='inner' # Solo queremos items de pedidos que existen
)

# 3. Filtrar por estados de pedido válidos
# Queremos ventas reales, no canceladas [cite: 179-180]
estados_validos = ['PAID', 'FULFILLED']
df_ventas = df_ventas[df_ventas['status'].isin(estados_validos)]

# 4. Unir con dim_tiempo para obtener el id_fecha
df_ventas = pd.merge(
    df_ventas,
    df_tiempo_dim[['id_fecha', 'fecha_completa']],
    left_on='fecha_join',
    right_on='fecha_completa',
    how='left'
)

# 5. Renombrar y seleccionar columnas (Modelado Final)
df_ventas_final = df_ventas.rename(columns={
    'order_item_id': 'id_item_pedido',
    'order_id': 'id_pedido',
    'id_fecha': 'id_fecha', # FK de dim_tiempo
    'order_date': 'timestamp_pedido', # Guardamos el timestamp original
    'customer_id': 'id_cliente', # FK de dim_cliente
    'channel_id': 'id_canal', # FK de dim_canal
    'store_id': 'id_tienda', # FK de dim_geografia
    'product_id': 'id_producto', # FK de dim_producto
    'quantity': 'cantidad',
    'unit_price': 'precio_unitario',
    'discount_amount': 'monto_descuento',
    'line_total': 'total_linea',
    'total_amount': 'total_pedido' 
})

# Seleccionar las columnas finales para la tabla de hechos
columnas_hechos = [
    'id_item_pedido',
    'id_pedido',
    'id_fecha',
    'timestamp_pedido',
    'id_cliente',
    'id_canal',
    'id_tienda',
    'id_producto',
    'cantidad',
    'precio_unitario',
    'monto_descuento',
    'total_linea',
    'total_pedido'
]
# Filtramos por las columnas deseadas y eliminamos duplicados si los hubiera
df_ventas_final = df_ventas_final[columnas_hechos].drop_duplicates()

# Guardar la tabla de hechos
ruta_salida = os.path.join(ruta_dw, 'fact_ventas.csv')
df_ventas_final.to_csv(ruta_salida, index=False)

print(f"Tabla de Hechos 'fact_ventas' creada y guardada en: {ruta_salida}")
print("--- Creación de Tabla de Hechos Finalizada ---")
print("\n*** ¡Todas las transformaciones han sido completadas! ***")