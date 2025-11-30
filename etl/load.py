import pandas as pd
import os

RUTA_DW = 'DW'


def save_to_dw(df, table_name):
    """Guarda un DataFrame en la carpeta DW."""

    os.makedirs(RUTA_DW, exist_ok=True)
    
    dw_path = os.path.join(RUTA_DW, f'{table_name}.csv')
    df.to_csv(dw_path, index=False)
    print(f"Datos guardados en: {dw_path}")
