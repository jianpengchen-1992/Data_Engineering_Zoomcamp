import json
import os
import io
from pathlib import Path
import pandas as pd
import logging

SETTINGS_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "settings.json"
MARKET_DATA_CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "market_data" / "market_data_configuration.json"

def load_json_config(relative_path=None):
    """
    Loads a JSON file relative to the Project Root.
    
    Args:
        relative_path (str/Path): The path to the file starting from project root. 
                                  If None, defaults to the standard market configuration.
    """
    # 1. Find the Project Root dynamically (works on any machine)
    #    Adjust .parent count depending on where THIS file lives.
    #    If this file is in src/utils/, we need .parent.parent
    current_script_dir = Path(__file__).resolve().parent
    project_root = current_script_dir.parent.parent 

    # 2. Set the Default Path if the user didn't provide one
    if relative_path is None:
        target_path = project_root / 'config' / 'market_data' / 'market_data_configuration.json'
    else:
        # If user provided a path, append it to the project root
        target_path = project_root / relative_path

    # 3. Check and Load
    if not target_path.exists():
        raise FileNotFoundError(f"Config file not found at: {target_path}")

    try:
        with open(target_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError:
        print(f"Error: '{target_path.name}' is not a valid JSON file.")
        return {}
    
def get_ids_from_json(json_data, target_main_cat, target_sub_cat, target_region):
    """
    Traverses the SMARD-style JSON to find data_ids based on a path string.
    
    Args:
        json_data (dict): The loaded JSON data.
        target_main_cat (string): the main catagory.
        target_sub_cat (string): sub catagory.
        target_region (string): DE, DE-LU, DE-LU-AU

    
    Returns:
        list: A list of found data_ids (integers).
    """
    target_main_cat = f"MM-Name.{target_main_cat}"
    target_sub_cat = f"MM-Name.{target_sub_cat}"

    found_ids = []

    # 2. Start traversing the 'main' list
    main_categories = json_data.get('main', [])
    
    for main in main_categories:
        # Check if the target category is part of the name (e.g. "Stromerzeugung" in "MM-Name.Stromerzeugung")
        if target_main_cat == main.get('name', ''):
            
            # 3. Traverse the 'sub' list
            sub_categories = main.get('sub', [])
            for sub in sub_categories:
                if target_sub_cat == sub.get('name', ''):
                    
                    # 4. Dig into module -> other
                    #    (The data is nested inside 'module' dictionary, under key 'other')

                    module_data = sub.get('module', {})
                    
                    defaults = module_data.get('default', [])
                    others = module_data.get('other', [])
                    
                    # Combine both lists
                    modules = defaults + others                   
                    
                    # 5. Filter the modules by Region
                    for mod in modules:
                        # The 'region' field is a list like ["DE", "AT", "DE-LU"]
                        # We check if our target_region exists in that list.
                        if target_region in mod.get('region', []):
                            
                            # Success! Add the ID to our list.
                            # We use 'data_id' as that is usually the API key, but you can change to 'id'.
                            found_ids.append(mod.get('id'))
                            
                            # Optional: Print details for debugging
                            #print(f"  [Match] Found '{mod.get('name')}' (ID: {mod.get('id')})")

    return found_ids

def create_payload(start_time, end_time, target_main_cat, target_sub_cat):
    """
    Creates the SMARD API payload using region/settings from config.
    Note: We removed 'target_region' from arguments because it's now in the config!
    """
    
    # 1. Load the Settings
    #    (Ideally load this once at top of file, but this works too)
    settings = load_json_config("config/settings.json")
    
    # 2. Extract the variables we need
    #    We need 'region' specifically to find the correct IDs
    template = settings["payload_template"]
    target_region = template["region"]  # This gets "DE-LU"
    
    # 3. Get the IDs using the Region from Config
    #    Now your ID search is locked to the same region as your payload
    market_config = load_json_config(MARKET_DATA_CONFIG_PATH)
    ids = get_ids_from_json(
        json_data=market_config,
        target_main_cat=target_main_cat,
        target_sub_cat=target_sub_cat,
        target_region=target_region 
    )

    # 4. Build the Request Object
    #    Start with the template (contains Format, Region, Type, Language)
    request_object = template.copy()
    
    #    Inject the dynamic parts
    request_object["moduleIds"] = ids
    request_object["timestamp_from"] = start_time
    request_object["timestamp_to"] = end_time

    # 5. Final Wrap
    payload = {
        "request_form": [request_object]
    }

    return payload, settings["api_url"]

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

from datetime import datetime
from zoneinfo import ZoneInfo

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

def date_to_timestamp_ms(dt_obj):
    """
    Takes a timezone-aware datetime object and returns unix milliseconds.
    """
    return int(dt_obj.timestamp() * 1000)


if __name__ == "__main__":
    pass
    
