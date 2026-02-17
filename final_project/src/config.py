import os
import json
from pathlib import Path
from dotenv import load_dotenv

# 1. Force load the .env file
# We use Path to find the root directory explicitly, so this works from ANYWHERE (notebooks, scripts, etc.)
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# 2. Assign variables to Python Constants
# This makes it easier to use elsewhere (your IDE will autocomplete 'GCP_PROJECT_ID')
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")

# 3. Handle the Key Path (Convert string to Path object for safety)
GCP_CREDENTIALS = os.getenv("GCP_CREDENTIALS")
if GCP_CREDENTIALS:
    # Make it absolute so Google's library doesn't get confused
    GCP_KEY_PATH = Path(__file__).resolve().parent.parent / GCP_CREDENTIALS
else:
    GCP_KEY_PATH = None

# 4. (Optional) Validation - Stop the program immediately if crucial keys are missing
if not GCP_BUCKET_NAME:
    raise ValueError("Error: GCP_BUCKET_NAME is missing in .env file!")
    
#---------

# 1. Find the path dynamically
BASE_DIR = Path(__file__).resolve().parent.parent
SETTINGS_PATH = BASE_DIR / "config" / "settings.json"

# 2. Load the settings
if not SETTINGS_PATH.exists():
    raise FileNotFoundError(f"Config file not found at {SETTINGS_PATH}")

with open(SETTINGS_PATH, "r") as f:
    _settings = json.load(f)

# 3. Expose the variables
API_URL = _settings["api_url"]
# We expose the whole 'defaults' dict so we can mix it easily later
API_DEFAULTS = _settings["payload_template"]