{{ config(
    materialized='view',
    schema='marts'
) }}

SELECT 
    timestamp_15min,
    DATE(timestamp_15min) AS energy_date,

    -- 1. The Wind Engine (Powered by BQML)
    -- We map the predicted column that BigQuery ML automatically generates
    actual_wind_generation AS gen_wind_actual,
    predicted_actual_wind_generation AS gen_wind_ml_predicted,

    -- 2. The Solar Engine (Powered by Geographic Weighting)
    -- We pass the solar data straight through the subquery below
    gen_solar_actual,
    
    -- A simple capacity-weighted index biased toward Southern Germany (feel free to tweak weights)
    (
        (COALESCE(weather__actual_solar_radiation_muenchen, 0) * 0.50) + 
        (COALESCE(weather__actual_solar_radiation_freiburg_im_breisgau, 0) * 0.30) + 
        (COALESCE(weather__actual_solar_radiation_berlin, 0) * 0.10) +
        (COALESCE(weather__actual_solar_radiation_hamburg, 0) * 0.10) 
    ) AS solar_radiation_index

FROM ML.PREDICT(
    MODEL `{{ target.database }}.{{ target.schema }}.wind_capacity_model`,
    (
        SELECT 
            timestamp_15min,
            -- Target
            (COALESCE(energy__actual_generation_wind_onshore, 0) + 
             COALESCE(energy__actual_generation_wind_offshore, 0)) AS actual_wind_generation,
            
            -- Solar Passthrough
            COALESCE(energy__actual_generation_photovoltaik, 0) AS gen_solar_actual,
            weather__actual_solar_radiation_muenchen,
            weather__actual_solar_radiation_freiburg_im_breisgau,
            weather__actual_solar_radiation_berlin,
            weather__actual_solar_radiation_hamburg,

            -- The Kitchen Sink (All your Wind Features from the macro go here)
            weather__actual_wind_speed_100m_rostock,
            weather__actual_wind_speed_100m_muenchen,
            weather__actual_wind_speed_100m_freiburg_im_breisgau,
            weather__actual_wind_speed_100m_hamburg,
            weather__actual_wind_speed_100m_kiel,
            weather__actual_wind_speed_100m_berlin,
            weather__actual_wind_direction_100m_rostock,
            weather__actual_wind_direction_100m_muenchen,
            weather__actual_wind_direction_100m_freiburg_im_breisgau,
            weather__actual_wind_direction_100m_hamburg,
            weather__actual_wind_direction_100m_kiel,
            weather__actual_wind_direction_100m_berlin,
            weather__actual_temperature_2m_rostock,
            weather__actual_temperature_2m_muenchen,
            weather__actual_temperature_2m_freiburg_im_breisgau,
            weather__actual_temperature_2m_hamburg,
            weather__actual_temperature_2m_kiel,
            weather__actual_temperature_2m_berlin

        FROM {{ ref('intermediate_energy_weather_joined') }}
    )
    
)
