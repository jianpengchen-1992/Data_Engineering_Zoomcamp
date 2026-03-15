import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry

from src.pipeline.weather.config import MINUTELY_15_VARIABLES_FORECAST, HOURLY_VARIABLES_HIST, URL_WEATHER_FORECAST, URL_WEATHER_HISTORICAL

def create_payload(lats, lons, start_date, end_date = None, target_cat = "historical"):# target_cat can be "historical" or "forecast"
    payload = {
        "latitude": lats,
        "longitude": lons,
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


def get_weather_dfs(api_url, params, cities):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)
    # 2. Fetch data
    dfs = []
    responses = openmeteo.weather_api(api_url, params=params)
    for city, r in zip(cities, responses):
        if "hourly" in params:
            json_data = r.Hourly()
            resolution = "hourly"
        else:
            json_data = r.Minutely15()
            resolution = "minutely_15"
        
        data = {"date": pd.date_range(
            start = pd.to_datetime(json_data.Time(), unit = "s", utc = True),
            end =  pd.to_datetime(json_data.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = json_data .Interval()),
            inclusive = "left"
         )}
        cols = params[resolution]
        for i, col in enumerate(cols):
            data[col] = json_data.Variables(i).ValuesAsNumpy()
        dataframe = pd.DataFrame(data = data)
        dataframe.insert(0, 'city', [city] * len(dataframe))
        dataframe['city'] = dataframe['city'].copy()
        # Force the 'date' column to nanosecond precision
        dataframe['date'] = pd.to_datetime(dataframe['date']).astype('datetime64[ms, UTC]')
        dfs.append(dataframe)
    return dfs


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
