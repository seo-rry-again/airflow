{{ config(
    materialized='incremental',
    unique_key=['fact_population_id'],
    incremental_strategy='merge',
    schema='fact'
) }}

WITH stg_population AS (

    SELECT
		source_id,
        area_code,
        area_name,
        congestion_label,
        population_min,
        population_max,
        male_population_ratio,
        female_population_ratio,
        age_0s_ratio,
        age_10s_ratio,
        age_20s_ratio,
        age_30s_ratio,
        age_40s_ratio,
        age_50s_ratio,
        age_60s_ratio,
        age_70s_ratio,
        resident_ratio,
        non_resident_ratio,
        is_replaced,
        observed_at,
        created_at
    FROM {{ ref('stg_population') }}

    {% if is_incremental() %}
        WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}

),

population_with_keys AS (
    SELECT
        sp.*,
        TO_CHAR(sp.observed_at, 'YYYYMMDDHH24MI')::BIGINT AS time_key
    FROM stg_population sp
),

final_fact AS (

    SELECT
		sp.source_id AS fact_population_id,
        sp.population_min,
        sp.population_max,
        sp.male_population_ratio,
        sp.female_population_ratio,
        sp.age_0s_ratio,
        sp.age_10s_ratio,
        sp.age_20s_ratio,
        sp.age_30s_ratio,
        sp.age_40s_ratio,
        sp.age_50s_ratio,
        sp.age_60s_ratio,
        sp.age_70s_ratio,
        sp.resident_ratio,
        sp.non_resident_ratio,
        sp.is_replaced,
        sp.observed_at,
        sp.created_at,
        CAST(da.area_id AS INT4) AS area_id,
        dc.congestion_id AS congestion_id,
        sp.time_key

    FROM population_with_keys sp
    LEFT JOIN dim.dim_congestion dc
        ON sp.congestion_label = dc.congestion_label
    LEFT JOIN dim.dim_time dt
        ON sp.time_key = dt.time_key
	LEFT JOIN dim.dim_area da
		ON sp.area_code = da.area_code
)

SELECT * FROM final_fact
