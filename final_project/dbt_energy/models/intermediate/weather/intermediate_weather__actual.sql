{{ config(
    materialized='incremental',
    unique_key='timestamp_15m', -- Tells dbt how to avoid duplicates
    partition_by={
      "field": "timestamp_15m",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=['timestamp_15m'] -- Sorts the data inside the daily partitions
) }}

WITH time_bounds AS (
    -- 1. DYNAMIC SPINE: Find the actual min/max dates from your staging data
    SELECT 
        MIN(`date`) AS start_time,
        -- We add 1 hour to the max time to ensure the last 15-min interval is generated
        TIMESTAMP_ADD(MAX(`date`), INTERVAL 1 HOUR) AS end_time
    FROM {{ ref('stg_weather__actual') }}
    
    -- 2. INCREMENTAL LOGIC: If this table already exists, only look at the last 3 days
    -- This saves MASSIVE money on daily Airflow runs
    {% if is_incremental() %}
        WHERE `date` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)
    {% endif %}
),

spine AS (
    -- 3. Use the dynamic bounds instead of hardcoded dates
    SELECT timestamp_15m 
    FROM time_bounds,
    UNNEST(GENERATE_TIMESTAMP_ARRAY(start_time, end_time, INTERVAL 15 MINUTE)) AS timestamp_15m
),

hourly_data AS (
    SELECT 
        city,
        `date`,
        temperature_2m,
        LEAD(temperature_2m) OVER (PARTITION BY city ORDER BY `date`) AS next_temperature,
        wind_speed_80m,
        LEAD(wind_speed_80m) OVER (PARTITION BY city ORDER BY `date`) AS next_wind_speed
    FROM {{ ref('stg_weather_1h') }}
    
    -- We also need the incremental logic here so we only process new weather data
    {% if is_incremental() %}
        WHERE`date` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)
    {% endif %}
),

interpolated_data AS (
    SELECT
        s.timestamp_15m,
        h.city,
        h.temperature_2m + ((h.next_temperature - h.temperature_2m) * (EXTRACT(MINUTE FROM s.timestamp_15m) / 60.0)) AS temp_15m,
        (h.wind_speed_80m + ((h.next_wind_speed - h.wind_speed_80m) * (EXTRACT(MINUTE FROM s.timestamp_15m) / 60.0))) * 1.05 AS wind_100m_15m
    FROM spine s
    LEFT JOIN hourly_data h
        ON DATE_TRUNC(s.timestamp_15m, HOUR) = h.`date`
    WHERE h.city IS NOT NULL 
)

SELECT * FROM interpolated_data
PIVOT (
    MAX(temp_15m) AS temp,
    MAX(wind_100m_15m) AS wind_100m
    FOR city IN ('Rostock', 'München', 'Freiburg im Breisgau', 'Hamburg', 'Kiel', 'Berlin') 
)