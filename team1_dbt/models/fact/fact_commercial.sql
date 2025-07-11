{{ config(
    materialized='incremental',
    unique_key=['fact_commercial_id'], 
    incremental_strategy='merge',      
    schema='fact'                      
) }}

WITH stg_commercial_data AS (
    SELECT
        source_id,             
        area_code,
        area_name,
        congestion_level,
        total_payment_count,
        payment_amount_min,
        payment_amount_max,
        male_ratio,
        female_ratio,
        age_10s_ratio,
        age_20s_ratio,
        age_30s_ratio,
        age_40s_ratio,
        age_50s_ratio,
        age_60s_ratio,
        individual_consumer_ratio,
        corporate_consumer_ratio,
        observed_at,            
        created_at               -- Incremental 로드의 기준 (원본 데이터 로드 시간)
    FROM {{ ref('stg_commercial') }}

    {% if is_incremental() %}
        -- 이전 실행 이후의 신규 데이터만 가져오기
        -- created_at (Source 테이블에 로드된 시간)을 기준으로 필터링
        WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
),

final_fact AS (
    SELECT
        source_id AS fact_commercial_id, 
        TO_CHAR(observed_at, 'YYYYMMDDHH24MI')::BIGINT AS time_key,
        
        congestion_level::VARCHAR(20) AS congestion_level,
        total_payment_count::INT AS total_payment_count,
        payment_amount_min::INT AS payment_amount_min,
        payment_amount_max::INT AS payment_amount_max,
        male_ratio::DECIMAL(5,2) AS male_ratio,
        female_ratio::DECIMAL(5,2) AS female_ratio,
        age_10s_ratio::DECIMAL(5,2) AS age_10s_ratio,
        age_20s_ratio::DECIMAL(5,2) AS age_20s_ratio,
        age_30s_ratio::DECIMAL(5,2) AS age_30s_ratio,
        age_40s_ratio::DECIMAL(5,2) AS age_40s_ratio,
        age_50s_ratio::DECIMAL(5,2) AS age_50s_ratio,
        age_60s_ratio::DECIMAL(5,2) AS age_60s_ratio,
        individual_consumer_ratio::DECIMAL(5,2) AS individual_consumer_ratio,
        corporate_consumer_ratio::DECIMAL(5,2) AS corporate_consumer_ratio,
        dc.area_id::SMALLINT AS area_id, 
        CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' AS created_at
    FROM stg_commercial_data sc
    LEFT JOIN {{ source('dim_data', 'area') }} dc
        ON sc.area_code = dc.area_code
)

SELECT * FROM final_fact
