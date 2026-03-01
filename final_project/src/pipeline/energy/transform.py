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
    clean_date_cols = [clean_bq_column_name(col) for col in chunk.columns]
    chunk.columns = clean_date_cols
    # 2. Convert date columns to UTC
    chunk[clean_date_cols[0]] = safe_convert_to_utc(chunk[clean_date_cols[0]])
    chunk[clean_date_cols[1]] = chunk[clean_date_cols[0]]+ pd.to_timedelta(15, unit = 'm') 

    return chunk

def energy_response_handler(response):
    """
    Reads the first line of the stream to get headers, 
    then returns the dtypes for Pandas.
    """

    
    # 2. Extract the column names
    columns = fields_from_response(response)
    
    # 3. Build your dynamic schema based on the columns you found
    parse_dates, schema = generate_parquet_schema_from_headers(columns)
    
    return {
        'dtype': schema,
        'parse_dates': parse_dates, # We MUST pass names, because we already consumed the header row!
        'names': columns 
    }
        


def generate_parquet_schema_from_headers(header_list):
    """
    Generates a schema with 2 timestamps and i floats(accroding to the amount_of_ids).
    """
    headers_of_number = header_list[2:]  # Assuming the first two are timestamps
    parse_dates = header_list[:2]  # Assuming the first two are timestamps
    schema = {}
    for col in headers_of_number:
        schema[col] = 'float64' 

    return parse_dates, schema

import io

def fields_from_response(response):
    """Extracts the 'fields' list from the API response stream.
    Reads exactly one line to avoid pulling the massive file into memory.
    """
    
    # Read strictly ONE line from the raw stream
    first_line = response.raw.readline()
    
    header_df = pd.read_csv(
        io.BytesIO(first_line), 
        sep=";", 
        encoding="utf-8-sig"
    )

    # Extract your clean list of columns
    header_list = header_df.columns.tolist()
    return header_list

