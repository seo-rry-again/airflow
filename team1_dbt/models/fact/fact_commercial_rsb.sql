{{ config(
    materialized='incremental',
    unique_key=['fact_commercial_rsb_id'], 
    incremental_strategy='merge',         
    schema='fact'                         
) }}

WITH stg_commercial_rsb_data AS (
    SELECT
        commercial_id,             
        source_id,        
        category_large,
        category_medium,
        category_congestion_level,
        category_payment_count,
        category_payment_min,  
        category_payment_max,  
        merchant_count,
        merchant_basis_month,
        created_at
    FROM {{ ref('stg_commercial_rsb') }}

    {% if is_incremental() %}
        WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
),

final_fact AS (
    SELECT
        s.source_id AS fact_commercial_id,
        {{ dbt_utils.generate_surrogate_key(['s.commercial_id', 's.source_id']) }}::VARCHAR(64) AS fact_commercial_rsb_id,
        s.category_congestion_level::VARCHAR(20) AS category_congestion_level,
        s.category_payment_count::INT AS category_payment_count,
        s.category_payment_min::INT AS category_payment_min, 
        s.category_payment_max::INT AS category_payment_max, 
        s.merchant_count::INT AS merchant_count,
        s.merchant_basis_month::CHARACTER(6) AS merchant_basis_month,
        dc.category_id::SMALLINT AS category_id,  
        CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' AS created_at
    FROM stg_commercial_rsb_data s
    LEFT JOIN {{ source('dim_data', 'category') }} dc
        ON s.category_large = dc.category_large
       AND s.category_medium = dc.category_medium
)

SELECT * FROM final_fact