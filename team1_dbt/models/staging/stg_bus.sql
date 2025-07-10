{{ config(
    materialized='view',
    schema='staging'
) }}

select
    source_id, area_code, area_name, total_geton_population_min, total_geton_population_max,
    total_getoff_population_min, total_getoff_population_max,
    geton_30min_population_min, geton_30min_population_max,
    getoff_30min_population_min, getoff_30min_population_max,
    geton_10min_population_min, geton_10min_population_max,
    getoff_10min_population_min, getoff_10min_population_max,
    geton_5min_population_min, geton_5min_population_max,
    getoff_5min_population_min, getoff_5min_population_max,
    station_count, station_count_basis_month,
    created_at, observed_at
from {{ source('raw_transport', 'bus') }}
