{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge',
    schema='dim'
) }}
with source_data as (
  select distinct
    event_name,
    to_date(split_part(event_period, '~', 1), 'YYYY-MM-DD') as event_start_date,
    to_date(split_part(event_period, '~', 2), 'YYYY-MM-DD') as event_end_date,
    event_place,
    longitude,
    latitude,
    is_paid,
    thumbnail_url,
    event_url,
    event_extra_detail,
    created_at
  from {{ ref('stg_event') }}
  {% if is_incremental() %}
    where created_at > (select max(created_at) from {{ this }})
  {% endif %}
)
select
  {{ dbt_utils.generate_surrogate_key([
       'event_name',
       'event_start_date',
       'event_end_date',
       'event_place',
       'longitude',
       'latitude',
       "case when is_paid then 'true' else 'false' end",
       'thumbnail_url',
       'event_url',
       'event_extra_detail'
     ]) }} as event_id,
  event_name,
  event_start_date,
  event_end_date,
  event_place,
  longitude,
  latitude,
  is_paid,
  thumbnail_url,
  event_url,
  event_extra_detail,
  current_timestamp at time zone 'Asia/Seoul' as created_at
from source_data
