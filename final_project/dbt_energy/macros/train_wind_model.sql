{% macro train_wind_model() %}

  {% set query %}
    CREATE OR REPLACE MODEL `your_project.your_dataset.wind_capacity_model`
    OPTIONS(model_type='linear_reg', input_label_cols=['actual_wind_generation']) AS
    
    SELECT 
        (COALESCE(energy__actual_generation_wind_onshore, 0) + 
         COALESCE(energy__actual_generation_wind_offshore, 0)) AS actual_wind_generation,
        weather__wind_speed_hamburg,
        weather__wind_speed_berlin,
        weather__wind_speed_munich,
        weather__wind_speed_frankfurt,
        weather__wind_speed_cologne,
        weather__wind_speed_stuttgart
    FROM {{ ref('intermediate_energy_weather_joined') }}
    -- Look back at a rolling 6-month window to capture recent seasonal trends
    WHERE DATE(timestamp_15min) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
      AND timestamp_15min IS NOT NULL
  {% endset %}

  {% do run_query(query) %}
  {{ log("BQML Wind Model successfully trained!", info=True) }}

{% endmacro %}