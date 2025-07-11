{{ config(
    materialized='view', 
    schema='staging'     
) }}

select
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
    created_at           
from {{ source('raw_commercial_data', 'commercial') }}