{{ config(
    materialized='incremental',
    unique_key='fact_event_id',
    incremental_strategy='merge',
    schema='fact'
) }}
with base as (
  select distinct
    event_name,
    event_period,
    to_date(split_part(event_period, '~', 1), 'YYYY-MM-DD') as event_start_date,
    to_date(split_part(event_period, '~', 2), 'YYYY-MM-DD') as event_end_date,
    event_place,
    longitude,
    latitude,
    is_paid,
    thumbnail_url,
    event_url,
    event_extra_detail,
    observed_at,
    created_at,
    area_code
  from {{ ref('stg_event') }}
  {% if is_incremental() %}
    where created_at > (select max(created_at) from {{ this }})
  {% endif %}
),
enriched as (
  select
    base.*,
    dim.event_id
  from base
  --dim_event 테이블에서 미리 생성해 둔 event_id(surrogate key)를 끌어오기 위해 사용
  left join {{ ref('dim_event') }} as dim
    on base.event_name        = dim.event_name
   and base.event_start_date = dim.event_start_date
   and base.event_end_date   = dim.event_end_date
   and base.event_place      = dim.event_place
),
with_surrogates as (
  select
    enriched.*,
    {{ dbt_utils.generate_surrogate_key([
       'event_id',
       'observed_at'
    ]) }} as fact_event_id
  from enriched
  where event_id is not null
)
select
  fact_event_id,
  event_period,
  current_timestamp at time zone 'Asia/Seoul' as created_at,
  event_id,
  --area_id 로 나중에 변경 필요
  area_code,
  observed_at
from with_surrogates
