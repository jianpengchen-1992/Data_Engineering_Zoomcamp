import pandas as pd
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
import io

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

def fields_from_response(response_json):
    """Extracts the 'fields' list from the API response JSON.
    This is useful for dynamically determining column names and types when converting to Parquet.
    """
    header_df = pd.read_csv(
    io.StringIO(response_json.text), 
    sep=";", 
    encoding="utf-8-sig", 
    nrows=0
    )

    # Extract your clean list of columns
    header_list = header_df.columns.tolist()
    return header_list

def safe_convert_to_utc(date_series: pd.Series, local_tz: str = 'Europe/Berlin') -> pd.Series:
    """
    Safely converts a naive datetime Series to UTC.
    Robustly handles Daylight Saving Time overlaps and gaps.
    """
    # 1. Ensure it is a datetime object
    s = pd.to_datetime(date_series)
    
    # 2. Localize with fallback logic
    try:
        # First attempt: Infer chronological order
        s = s.dt.tz_localize(local_tz, ambiguous='infer', nonexistent='shift_forward')
    except Exception as e:
        # Fallback: Out-of-order data or bad chunk slice
        logging.warning(f"Timezone inference failed for {date_series.name}. Falling back to NaT. Detail: {e}")
        s = s.dt.tz_localize(local_tz, ambiguous='NaT', nonexistent='shift_forward')
        
    # 3. Convert to UTC and return
    return s.dt.tz_convert('UTC')

def date_to_timestamp_ms(dt_obj):
    """
    Takes a timezone-aware datetime object and returns unix milliseconds.
    """
    return int(dt_obj.timestamp() * 1000)

def parse_german_date(date_string):
    """
    Parses a string (DD.MM.YYYY or DD/MM/YYYY) into a timezone-aware 
    datetime object for Europe/Berlin.
    """
    tz = ZoneInfo("Europe/Berlin")
    
    # standardize separator
    clean_date = date_string.replace('/', '.')
    
    # Parse to naive datetime
    naive_dt = datetime.strptime(clean_date, "%d.%m.%Y")
    
    # Make it timezone-aware (This handles the specific offset for that day)
    return naive_dt.replace(tzinfo=tz)
