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

WITH time_bounds AS (
    -- 1. DYNAMIC SPINE: Find the actual min/max dates from your staging data
    SELECT 
        MIN(`weather_timestamp`) AS start_time,
        -- We add 1 hour to the max time to ensure the last 15-min interval is generated
        MAX(`weather_timestamp`) AS end_time
    FROM {{ ref('stg_weather__actual') }}
    
    -- 2. INCREMENTAL LOGIC: If this table already exists, only look at the last 3 days
    -- This saves MASSIVE money on daily Airflow runs
    {% if is_incremental() %}
        WHERE `weather_timestamp` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)
    {% endif %}
),

spine AS (
    -- 3. Use the dynamic bounds instead of hardcoded dates
    SELECT timestamp_15min 
    FROM time_bounds,
    UNNEST(GENERATE_TIMESTAMP_ARRAY(start_time, end_time, INTERVAL 15 MINUTE)) AS timestamp_15min
),

{% set weather_metrics_actual = [
    'temperature_2m',
    'wind_speed_100m',
    'wind_direction_100m',
    'snowfall',
    'precipitation',
    'solar_radiation',
    'direct_radiation',
    'diffuse_radiation'
] %}

hourly_data AS (
    SELECT 
        city,
        `weather_timestamp`
        {{ generate_lead_columns(weather_metrics_actual, 'city', 'weather_timestamp') }}

    FROM {{ ref('stg_weather__actual') }}
    
    -- We also need the incremental logic here so we only process new weather data
    {% if is_incremental() %}
        WHERE`weather_timestamp` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)
    {% endif %}
),

interpolated_data AS (
    SELECT
        s.timestamp_15min,
        h.city
        
        -- Loop through your global list of metrics
        {% for metric in weather_metrics_actual %}
        
        -- Call the math macro and alias the new column
        , {{ interpolate_linear(metric, 's.timestamp_15min') }} AS {{ metric }}_15min
        
        {% endfor %}
        
        FROM spine s
    LEFT JOIN hourly_data h
        ON DATE_TRUNC(s.timestamp_15min, HOUR) = h.`weather_timestamp`
    WHERE h.city IS NOT NULL 
)

SELECT * FROM interpolated_data
PIVOT (
    
    -- LOOP 1: The Metrics (Aggregations)
    {% for metric in weather_metrics_actual %}
        MAX({{ metric }}_15min) AS {{ metric }}{% if not loop.last %}, {% endif %}
    {% endfor %}
    
    FOR city IN (
        
        -- LOOP 2: The Cities (Values)
        {% for city in var('target_cities') %}
            '{{ city }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
        
    ) 
)