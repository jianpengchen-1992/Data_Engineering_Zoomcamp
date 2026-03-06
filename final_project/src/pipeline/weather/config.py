CITIES = [
    {"name": "Kiel", "lat": 54.323334, "lon": 10.139444},
    {"name": "Hamburg", "lat": 53.541328, "lon": 9.984355},
    {"name": "Rostock", "lat": 54.083336, "lon": 12.108811},
    {"name": "München", "lat": 48.137154, "lon": 11.576124},
    {"name": "Freiburg im Breisgau", "lat": 47.995609, "lon": 7.852736},
    {"name": "Berlin", "lat": 52.506687, "lon": 13.383505}
    #add a new dictionary here to add a city
]
# You can easily add "relative_humidity_2m", "precipitation", etc.

#the hourly variables are the same for current data, so we can reuse the same list for both current and historical data(1 hour).
HOURLY_VARIABLES_HIST = ["temperature_2m", 
                    "wind_speed_100m",
                    "wind_direction_100m", 
                    "snowfall", 
                    "precipitation", 
                    "shortwave_radiation", 
                    "direct_radiation", 
                    "diffuse_radiation"
                    ]

MINUTELY_15_VARIABLES_FORECAST = ["temperature_2m", 
                    "wind_speed_80m", 
                    "wind_direction_80m", 
                    "snowfall", 
                    "precipitation", 
                    "shortwave_radiation", 
                    "direct_radiation", 
                    "diffuse_radiation"
                    ]

WEATHER_CSV_SETTING = {
    'sep': ',', 
    'decimal': '.', 
    'thousands': ',', 
    'date_format': '%d.%m.%Y  %H:%M', 
    'header': 0, 
    'encoding': 'utf-8-sig', 
    'na_values': ['-']
}

URL_WEATHER_HISTORICAL = "https://archive-api.open-meteo.com/v1/archive"
URL_WEATHER_FORECAST =  "https://api.open-meteo.com/v1/forecast"

