{{ config(
    materialized='view',
    schema='staging'
) }}

with source as (
    select * from {{ source('weather_data', 'forecasted_weather') }}
),

renamed as (
    select
        -- Renaming happens here using standard SQL aliases
        cast(city as string) as `city`,
        cast(`date` as timestamp) as `weather_timestamp`, -- Renaming to avoid reserved keyword issues
        -- 2. Renaming a field (using standard snake_case without backticks for safety)
        
        cast(round(temperature_2m, 1) as NUMERIC) as `temperature_2m`,
        cast(round(wind_speed_80m * 1.05, 1) as NUMERIC) as `wind_speed_100m`,
        cast(round(wind_direction_80m, 1) as INT64) as `wind_direction_100m`,
        cast(round(snowfall, 1) as NUMERIC) as `snowfall`,
        cast(round(precipitation, 1) as NUMERIC) as `precipitation`,
        cast(round(shortwave_radiation, 1) as NUMERIC) as `solar_radiation`,
        cast(round(direct_radiation, 1) as NUMERIC) as `direct_radiation`,
        cast(round(diffuse_radiation, 1) as NUMERIC) as `diffuse_radiation`        

    from source
)

select * from renamed