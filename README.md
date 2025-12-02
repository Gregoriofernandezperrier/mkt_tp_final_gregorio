# Trabajo Práctico Final - Ecosistema de Datos (EcoBottle AR)

**Autor:** Gregorio Fernández Perrier

Proyecto final de "Introducción al Marketing Online y los Negocios Digitales". El objetivo es implementar un pipeline de ETL (Extract, Transform, Load) que toma datos crudos (`RAW/`), los modela en una Constelación de Hechos (Esquema Estrella) usando el script `transform.py`, y los carga en un Data Warehouse (carpeta `DW/`) listo para ser analizado.

---

## 0. Contenidos

1.  [Descripción y Objetivos](#1-descripción-y-objetivos)
2.  [Modelo de Datos y Supuestos](#2-modelo-de-datos-y-supuestos)
3.  [Diagramas del Esquema (Constelación de Hechos)](#3-diagramas-del-esquema-constelación-de-hechos)
4.  [Estructura del Repositorio](#4-estructura-del-repositorio)
5.  [Instrucciones de Ejecución](#5-instrucciones-de-ejecución)
6.  [Diccionario de Datos (Data Warehouse)](#6-diccionario-de-datos-data-warehouse)
7.  [KPIs del Dashboard (Próxima Entrega)](#7-kpis-del-dashboard-próxima-entrega)

---

## 1. Descripción y Objetivos

El proyecto implementa un pipeline ETL modular que toma los datos crudos ubicados en `RAW/`, los transforma siguiendo un modelo dimensional (Esquema Estrella) y genera un Data Warehouse en formato CSV dentro de la carpeta `DW/`.

El pipeline está dividido en tres etapas:

- **Extracción** (`etl/extract.py`)
- **Transformación** (creación de dimensiones y hechos, `etl/transform.py`)
- **Carga** (`etl/load.py`)

Este diseño permite reprocesar cada tabla de manera independiente y está alineado con las prácticas vistas en clase.

Todas las dimensiones del modelo incluyen **claves sustitutas** (`*_sk`) generadas durante el ETL, mientras que las claves naturales originales (`id_*`) se mantienen como atributos para trazabilidad.

El Data Warehouse resultante es utilizado en la construcción del dashboard de KPIs solicitados en Power BI.


## 2. Modelo de Datos y Supuestos

El Data Warehouse sigue un diseño de **Constelación de Hechos**, donde varias tablas de hechos comparten dimensiones conformadas. El objetivo es obtener un modelo analítico limpio, consultable y alineado con las prácticas vistas en clase.

### ✔️ Claves del Modelo
- Todas las **dimensiones** utilizan **claves sustitutas** (`*_sk`) generadas durante el proceso de transformación.
- Las **claves naturales** provenientes de `RAW` (por ejemplo `id_product`, `id_customer`, `id_channel`) se mantienen como atributos informativos, pero **ya no forman parte de las relaciones** entre tablas.
- Las **tablas de hechos** referencian únicamente las **surrogate keys** de sus dimensiones.

### ✔️ Dimensión de Tiempo (`dim_date`)
- Se genera automáticamente en base al rango de fechas de `sales_order`.
- Aporta atributos como año, mes, trimestre, nombre del mes, día de la semana, etc.
- `id_date` (formato `YYYYMMDD`) funciona como surrogate key de la dimensión.

### ✔️ Denormalización en Dimensiones
- **`dim_product`** integra la categoría del producto a partir de `product_category`.
- **`dim_location`** incluye información de dirección y código postal.
- **`dim_store`** incorpora su localización mediante `address_id`.

### ✔️ Supuestos Relevantes del Negocio
- Solo se consideran **ventas válidas** las órdenes con estado `PAID` o `FULFILLED`.
- Las sesiones web sin cliente asociado se mantienen, pero sólo generan `customer_sk` cuando corresponde.
- Los pagos y envíos se modelan como hechos independientes por transacción/logística.

En conjunto, este modelo permite analizar ventas, sesiones, NPS, logística y pagos con un nivel de detalle consistente y unificado a través de dimensiones conformadas.

## 3. Diagramas del Modelo (Constelación de Hechos)

Los siguientes diagramas ilustran las relaciones entre las dimensiones y las tablas de hechos del Data Warehouse.  
Se encuentran en la carpeta `esquemas_estrella/` y representan el modelo implementado en el ETL, utilizando claves sustitutas en todas las dimensiones.

### A. Constelación de Ventas (Pedidos, Items, Pagos y Envíos)
![Diagrama de Ventas](esquemas_estrella/ventas.jpeg)

### B. Esquema de Sesiones Web
![Diagrama de Sesiones](esquemas_estrella/web_session.jpeg)

### C. Esquema de NPS
![Diagrama de NPS](esquemas_estrella/NPS.jpeg)

### D. Esquemas de Pagos y Envíos
![Diagrama de Pagos](esquemas_estrella/payments.jpeg)
![Diagrama de Envíos](esquemas_estrella/shipments.jpeg)

## 4. Estructura del Repositorio

El proyecto fue reorganizado siguiendo un enfoque modular del pipeline ETL (Extract – Transform – Load).  
Esto permite ejecutar, mantener y testear cada etapa de manera independiente.

.
├── README.md
├── requirements.txt
├── .gitignore
├── main.py 
│
├── etl/ 
│ ├── extract.py 
│ ├── transform.py 
│ └── load.py
│
├── RAW/
│ └── .csv
│
├── DW/
│ ├── dim_.csv
│ └── fact_*.csv
│
├── esquemas_estrella/ # Diagramas del modelo dimensional
│ └── *.jpeg
│
└── .venv/


## 5. Instrucciones de Ejecución

Para ejecutar este proyecto y regenerar el Data Warehouse en tu máquina local usando el orquestador `main.py`:

1.  **Clonar el repositorio:**
    ```bash
    git clone [https://github.com/Gregoriofernandezperrier/mkt_tp_final_gregorio.git](https://github.com/Gregoriofernandezperrier/mkt_tp_final_gregorio.git)
    cd mkt_tp_final_gregorio
    ```

2.  **Crear y activar un entorno virtual:**
    ```bash
    # Crear el entorno
    python -m venv .venv

    # Activar en Windows (Command Prompt)
    .\.venv\Scripts\activate
    
    # Activar en macOS/Linux
    # source .venv/bin/activate
    ```

3.  **Instalar las dependencias:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Ejecutar el Pipeline ETL:**
    El archivo `main.py` orquesta las tres etapas del proceso (extract, transform y load).  
    Asegúrate de que la carpeta `RAW/` contenga todos los CSVs originales.

    ```bash
    python main.py
    ```

Tras la ejecución, la carpeta `DW/` (que se crea automáticamente) contendrá las 13 tablas del Esquema Estrella en formato `.csv`, listas para ser consumidas por Power BI.

## 6. Diccionario de Datos (Data Warehouse)

El pipeline genera las siguientes 13 tablas en la carpeta `DW/`.

### A. Dimensiones (Dims)

| Tabla | Descripción |
| :--- | :--- |
| **`dim_channel`** | Catálogo de canales de venta (Online, Offline). |
| **`dim_customer`** | Maestro de clientes (incluye `full_name`). |
| **`dim_date`** | Dimensión de calendario generada (Día, Mes, Año, etc.). |
| **`dim_location`** | Maestro de direcciones (une `address` y `province`). |
| **`dim_product`** | Maestro de productos (une `product` y `category`). |
| **`dim_province`** | Catálogo de provincias. |
| **`dim_store`** | Maestro de tiendas físicas y sus direcciones. |

Nota:
Todas las dimensiones incorporan claves sustitutas (*_sk) generadas durante el proceso ETL.
Las claves naturales del sistema transaccional (id_*) se mantienen como atributos descriptivos.

### B. Hechos (Facts)

| Tabla | Descripción | Grano |
| :--- | :--- | :--- |
| **`fact_sales_order`** | Cabeceras de las órdenes de venta. | Una fila por orden. |
| **`fact_sales_order_item`** | Detalle de productos en cada orden. | Una fila por producto dentro de una orden. |
| **`fact_payment`** | Registra las transacciones de pago. | Una fila por transacción de pago. |
| **`fact_shipment`** | Registra la información logística de envíos. | Una fila por envío. |
| **`fact_web_session`** | Sesiones de navegación web. | Una fila por sesión web. |
| **`fact_nps_response`** | Respuestas a las encuestas de NPS. | Una fila por respuesta de encuesta. |

Nota:
Las tablas de hechos referencian las dimensiones utilizando las surrogate keys (*_sk) y el atributo id_date en el caso de la dimensión calendario.
---

##---

---

---

## 7. Link al Dashboard Final

https://app.powerbi.com/links/aAI72hu5ar?ctid=3e0513d6-68fa-416e-8de1-6c5cdc319ffa&pbi_source=linkShare