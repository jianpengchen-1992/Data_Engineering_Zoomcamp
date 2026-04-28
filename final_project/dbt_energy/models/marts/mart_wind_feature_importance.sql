{{ config(
    materialized='table',
    schema='marts'
) }}

WITH raw_explain AS (
    SELECT 
        feature AS weather_column,
        attribution AS raw_score
    FROM ML.GLOBAL_EXPLAIN(MODEL `{{ target.database }}.{{ target.schema }}.wind_capacity_model`)
)

SELECT 
    weather_column,
    ROUND((raw_score / SUM(raw_score) OVER()) * 100, 2) AS importance_percentage
FROM raw_explain
ORDER BY importance_percentage DESC