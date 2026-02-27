import pandas as pd
import re
from src.utils.data_helper import safe_convert_to_utc   
def clean_bq_column_name(name):
    # 1. Handle German Umlauts
    replacements = {'ä': 'ae', 'ö': 'oe', 'ü': 'ue', 'ß': 'ss', 'Ä': 'Ae', 'Ö': 'Oe', 'Ü': 'Ue'}
    for char, rep in replacements.items():
        name = name.replace(char, rep)
    
    # 2. Replace everything that isn't a letter, number, or underscore with '_'
    name = re.sub(r'[^a-zA-Z0-9]', '_', name)
    
    # 3. BigQuery doesn't like double underscores like '__' from ' [MWh]'
    name = re.sub(r'_+', '_', name).strip('_')
    
    return name
def transform_energy_chunk(chunk):
    # 1. Standardize column names
    chunk.columns = [clean_bq_column_name(col) for col in chunk.columns]
    
    # 2. Figure out the schema dynamically (only need to calculate dates once)
    # Since Pandas already loaded it, we can just look at chunk.columns
    date_cols = [col for col in chunk.columns if 'time' in col.lower() or 'date' in col.lower()]
    
    # 3. Apply your specific date conversions
    if len(date_cols) >= 2:
        chunk[date_cols[0]] = safe_convert_to_utc(pd.to_datetime(chunk[date_cols[0]]))
        chunk[date_cols[1]] = chunk[date_cols[0]] + pd.to_timedelta(15, unit='m')
        
    return chunk