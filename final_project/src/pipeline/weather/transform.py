iimport requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import io
import json

def get_weather_json_file(url, params):
    # Pro move: Define a retry strategy so your code doesn't 
    # crash on a 500 error or rate limit (429)
    retry_strategy = Retry(
        total=5,
        backoff_factor=1, # Wait 1s, 2s, 4s...
        status_forcelist=[429, 500, 502, 503, 504],
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)

    # Now you have a robust requester
    response = session.get(url, params=params)
    response.raise_for_status()
    
    # Get the raw JSON dictionary
    data = response.json()

    # Wrap it in a virtual file for your GCS uploader
    virtual_file = io.BytesIO(json.dumps(data).encode('utf-8'))
    virtual_file.seek(0)
    
    return virtual_file

