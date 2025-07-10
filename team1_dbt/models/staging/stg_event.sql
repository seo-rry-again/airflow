{{ config(
    materialized='view',
    schema='staging'
) }}
with renamed as (
  select
    area_code,
    area_name,
    event_name,
    event_period,
    event_place,
    cast(event_x as double precision) as longitude,
    cast(event_y as double precision) as latitude,
    is_paid,
    thumbnail_url,
    event_url,
    event_extra_detail,
    observed_at,
    created_at
  from {{ source('raw_event_data', 'source_event') }}
)
select * from renamed
