{{ config(
    materialized='view',
    schema='marts'
) }}

SELECT 
    -- 1. The Time Axes
    timestamp_15min,
    DATE(timestamp_15min) AS energy_date, -- We keep this so the dashboard Date Filter still works!

    -- 2. The Price Line (The Right Axis)
    -- Assuming your price is hourly, it will naturally repeat for the four 15-min blocks.
    market__wholesale_prices_de_lu AS price_de_lu,

    -- 3. The Generation Stack (The Left Axis)
    -- Layer 1: Nuclear (Base Load - stays flat at the bottom)
    COALESCE(energy__actual_generation_kernenergie, 0) AS gen_nuclear,

    -- Layer 2: Fossil Fuels (Coal & Gas - flexes to meet demand)
    (
        COALESCE(energy__actual_generation_braunkohle, 0) + 
        COALESCE(energy__actual_generation_steinkohle, 0) + 
        COALESCE(energy__actual_generation_erdgas, 0)
    ) AS gen_fossil,

    -- Layer 3: Dispatchable Renewables (Hydro & Biomass)
    (
        COALESCE(energy__actual_generation_biomasse, 0) + 
        COALESCE(energy__actual_generation_wasserkraft, 0)+
        COALESCE(energy__actual_generation_sonstige_erneuerbare, 0)
    ) AS gen_dispatchable_renewable,

    -- Layer 4: Volatile Renewables (Wind & Solar - The ones that crush the price)
    (
        COALESCE(energy__actual_generation_wind_onshore, 0) + 
        COALESCE(energy__actual_generation_wind_offshore, 0)
    ) AS gen_wind,
    
    COALESCE(energy__actual_generation_photovoltaik, 0) AS gen_solar

FROM {{ ref('intermediate_energy_weather_joined') }}