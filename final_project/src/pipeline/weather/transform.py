import requests
import pandas as pd

from src.pipeline.weather.config import MINUTELY_15_VARIABLES_FORECAST, HOURLY_VARIABLES_HIST, URL_WEATHER_FORECAST, URL_WEATHER_HISTORICAL

def create_payload(lat, lon, start_date, end_date = None, target_cat = "historical"):# target_cat can be "historical" or "forecast"
    payload = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
    }

    if target_cat == "historical":
        payload["hourly"] = HOURLY_VARIABLES_HIST
        api_url = URL_WEATHER_HISTORICAL
    elif target_cat == "forecast":
        payload["minutely_15"] = MINUTELY_15_VARIABLES_FORECAST
        api_url = URL_WEATHER_FORECAST
    else:
        raise ValueError("Invalid target category. Must be 'historical' or 'forecast'.")
    return payload, api_url


def get_weather_csv_stream(api_url, params, response_handler = None, csv_kwargs=None):
# 1. Safe initialization of dictionary
    if csv_kwargs is None:
        csv_kwargs = {}
    
    # 2. Fetch data
    r = requests.get(api_url, params=params)
    r.raise_for_status()  # Professional touch: crash early if the API is down
    data = r.json()
    
    # 3. Extract the right key (using .get() is safer)
    payload = data.get('hourly') or data.get('minutely_15')
    if payload is None:
        raise ValueError("API response missing 'hourly' or 'minutely_15' keys")
        
    df = pd.DataFrame(payload)
    
    # 4. Handle dynamic updates
    if response_handler:
        # Use the passed function, not a hardcoded name
        dynamic_kwargs = response_handler(df)
        csv_kwargs.update(dynamic_kwargs)
    
    # 5. Return as list of strings (as requested by your .splitlines() logic)
    return df.to_csv(index=False, **csv_kwargs).splitlines()


def weather_response_handler(df):
    """
    Reads the first line of the stream to get headers, 
    then returns the dtypes for Pandas.
    """

    
    # 2. Extract the column names
    columns = df.columns.tolist()
    
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
    headers_of_number = header_list[1:]  # Assuming the first is timestamp
    parse_dates = header_list[:1]  # Assuming the first is timestamp
    schema = {}
    for col in headers_of_number:
        schema[col] = 'float64' 

    return parse_dates, schema
