{{ config(
    materialized='view', 
    schema='staging'     
) }}

select
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
    observed_at,
    created_at         
from {{ source('raw_commercial_data', 'commercial_rsb') }}