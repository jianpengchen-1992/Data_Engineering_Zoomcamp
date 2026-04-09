{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key='timestamp_15min',
    partition_by={
      "field": "timestamp_15min",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=['timestamp_15min']
) }}

{% set weather_metrics_forecast = [
    'temperature_2m',
    'wind_speed_100m',
    'wind_direction_100m',
    'snowfall',
    'precipitation',
    'solar_radiation',
    'direct_radiation',
    'diffuse_radiation'
] %}

with weather_data_15min AS (
    SELECT 
        city,
        `weather_timestamp` AS timestamp_15min
        {% for metric in weather_metrics_forecast %}
            ,{{ metric }} AS {{ metric }}_15min
        {% endfor %}

    FROM {{ ref('stg_weather__forecast_15min') }}
    
    -- We also need the incremental logic here so we only process new weather data
    {% if is_incremental() %}
        WHERE`weather_timestamp` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)
    {% endif %}
)

SELECT * FROM weather_data_15min
PIVOT (
    
    -- LOOP 1: The Metrics (Aggregations)
    {% for metric in weather_metrics_forecast %}
        MAX({{ metric }}_15min) AS {{ metric }}{% if not loop.last %}, {% endif %}
    {% endfor %}
    
    FOR city IN (
        
        -- LOOP 2: The Cities (Values)
        {% for city in var('target_cities') %}
            '{{ city }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
        
    ) 
)