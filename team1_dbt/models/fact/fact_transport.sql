{{ config(
    materialized='incremental',
    unique_key='fact_transport_id',
    incremental_strategy='merge',
    schema='fact'
) }}

with bus as (
    select
        source_id,
        area_code,
        area_name,
        total_geton_population_min,
        total_geton_population_max,
        total_getoff_population_min,
        total_getoff_population_max,
        geton_30min_population_min,
        geton_30min_population_max,
        getoff_30min_population_min,
        getoff_30min_population_max,
        geton_10min_population_min,
        geton_10min_population_max,
        getoff_10min_population_min,
        getoff_10min_population_max,
        geton_5min_population_min,
        geton_5min_population_max,
        getoff_5min_population_min,
        getoff_5min_population_max,
        station_count,
        station_count_basis_month,
        created_at,
        observed_at
    from {{ ref('stg_bus') }}

    {% if is_incremental() %}
        where created_at > (select max(created_at) from {{ this }})
    {% endif %}
),

subway as (
    select
        source_id,
        area_code,
        area_name,
        total_geton_population_min,
        total_geton_population_max,
        total_getoff_population_min,
        total_getoff_population_max,
        geton_30min_population_min,
        geton_30min_population_max,
        getoff_30min_population_min,
        getoff_30min_population_max,
        geton_10min_population_min,
        geton_10min_population_max,
        getoff_10min_population_min,
        getoff_10min_population_max,
        geton_5min_population_min,
        geton_5min_population_max,
        getoff_5min_population_min,
        getoff_5min_population_max,
        station_count,
        station_count_basis_month,
        created_at,
        observed_at
    from {{ ref('stg_subway') }}

    {% if is_incremental() %}
        where created_at > (select max(created_at) from {{ this }})
    {% endif %}
),

unioned as (
    select 'bus' as transport_type, * from bus
    union all
    select 'subway' as transport_type, * from subway
),

final_fact as (
    select
        source_id || '-' || transport_type as fact_transport_id,
        transport_type,
        station_count_basis_month,
        total_geton_population_min,
        total_geton_population_max,
        total_getoff_population_min,
        total_getoff_population_max,
        geton_30min_population_min,
        geton_30min_population_max,
        getoff_30min_population_min,
        getoff_30min_population_max,
        geton_10min_population_min,
        geton_10min_population_max,
        getoff_10min_population_min,
        getoff_10min_population_max,
        geton_5min_population_min,
        geton_5min_population_max,
        getoff_5min_population_min,
        getoff_5min_population_max,
        station_count,
        area_code as area_id,
        to_char(observed_at, 'YYYYMMDDHH24MI')::bigint as time_key,
        getdate() as created_at
    from unioned
)

select * from final_fact
