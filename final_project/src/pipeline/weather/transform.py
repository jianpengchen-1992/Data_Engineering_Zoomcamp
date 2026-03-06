import requests

from src.pipeline.weather.config import MINUTELY_15_VARIABLES_HIST, HOURLY_VARIABLES_HIST, URL_WEATHER_FORECAST, URL_WEATHER_HISTORICAL

def get_weather_csv_stream(api_url, params):
    r = requests.get(api_url, params=params)
    data = r.json()
    df = pd.DataFrame(data['minutely_15'])
    return df.to_csv(index=False).splitlines()

def create_payload(start_date, end_date = None, lat, lon, target_cat = "historical"):
    if end_date is None:
        end_date = start_date
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
        payload["minutely_15"] = MINUTELY_15_VARIABLES_HIST
        api_url = URL_WEATHER_FORECAST
    else:
        raise ValueError("Invalid target category. Must be 'historical' or 'forecast'.")
    return payload, api_url

