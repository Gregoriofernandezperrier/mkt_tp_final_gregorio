import pandas as pd
import os

RUTA_RAW = 'RAW'


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
