{% macro train_wind_model() %}

  {% set query %}
    CREATE OR REPLACE MODEL `{{ target.database }}.{{ target.schema }}.wind_capacity_model`
    OPTIONS(
        model_type='boosted_tree_regressor', 
        input_label_cols=['actual_wind_generation'],
        enable_global_explain=TRUE
    ) AS
    
    SELECT 
        -- THE TARGET (What we want to predict)
        (COALESCE(energy__actual_generation_wind_onshore, 0) + 
         COALESCE(energy__actual_generation_wind_offshore, 0)) AS actual_wind_generation,
        
        -- THE KITCHEN SINK (The Features)
        -- 1. Wind Speeds
        weather__actual_wind_speed_100m_rostock,
        weather__actual_wind_speed_100m_muenchen,
        weather__actual_wind_speed_100m_freiburg_im_breisgau,
        weather__actual_wind_speed_100m_hamburg,
        weather__actual_wind_speed_100m_kiel,
        weather__actual_wind_speed_100m_berlin,

        -- 2. Wind Directions
        weather__actual_wind_direction_100m_rostock,
        weather__actual_wind_direction_100m_muenchen,
        weather__actual_wind_direction_100m_freiburg_im_breisgau,
        weather__actual_wind_direction_100m_hamburg,
        weather__actual_wind_direction_100m_kiel,
        weather__actual_wind_direction_100m_berlin,

        -- 3. Temperatures (Let the tree figure out if cold air = denser air = more wind power!)
        weather__actual_temperature_2m_rostock,
        weather__actual_temperature_2m_muenchen,
        weather__actual_temperature_2m_freiburg_im_breisgau,
        weather__actual_temperature_2m_hamburg,
        weather__actual_temperature_2m_kiel,
        weather__actual_temperature_2m_berlin

    FROM {{ ref('intermediate_energy_weather_joined') }}
    WHERE DATE(timestamp_15min) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
      AND timestamp_15min IS NOT NULL
  {% endset %}

  {% do run_query(query) %}
  {{ log("BQML Boosted Tree Model successfully trained on the Kitchen Sink!", info=True) }}

{% endmacro %}