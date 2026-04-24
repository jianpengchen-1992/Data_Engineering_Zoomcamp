{{ config(
    materialized='view',
    schema='marts'
) }}

SELECT 
    timestamp_15min,
    DATE(timestamp_15min) AS energy_date,

    -- 1. The Wind Engine
    (
        COALESCE(energy__actual_generation_wind_onshore, 0) + 
        COALESCE(energy__actual_generation_wind_offshore, 0)
    ) AS gen_wind,
    weather__actual_wind_speed AS wind_speed, -- Adjust name if your OBT uses a different alias

    -- 2. The Solar Engine
    COALESCE(energy__actual_generation_photovoltaik, 0) AS gen_solar,
    
    -- Shortwave radiation is the best metric for solar panel output
    weather__actual_shortwave_radiation AS solar_radiation 

FROM {{ ref('intermediate_energy_weather_joined') }}
WHERE timestamp_15min IS NOT NULL