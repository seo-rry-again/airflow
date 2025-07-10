{{ config(
    unique_key=['fact_weather_id'],
    incremental_strategy='merge',
) }}

WITH stg_weather AS (
    SELECT
        source_id,
        area_code,
        area_name,
        temperature,
        sensible_temperature,
        max_temperature,
        min_temperature,
        humidity,
        wind_direction,
        wind_speed,
        precipitation,
        precipitation_type,
        precipitation_message,
        sunrise,
        sunset,
        uv_index_level,
        uv_index_desc,
        uv_message,
        pm25_index,
        pm25_value,
        pm10_index,
        pm10_value,
        air_quality_index,
        air_quality_value,
        air_quality_main,
        air_quality_message,
        observed_at,
        created_at
    FROM {{ ref('stg_weather') }}
    {% if is_incremental() %}
        WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
),
fact AS (
    SELECT
        source_id AS fact_weather_id,
        temperature,
        sensible_temperature,
        max_temperature,
        min_temperature,
        humidity,
        wind_direction AS wind_direction_code,
        wind_speed,
        precipitation_type,
        precipitation AS precipitation_amount,
        precipitation_message,
        uv_index_level,
        uv_index_desc,
        uv_message,
        pm25_value,
        pm25_index,
        pm10_value,
        pm10_index,
        air_quality_index,
        air_quality_value,
        air_quality_main,
        air_quality_message,
        sunrise,
        sunset,
        area_code  AS area_id,
        CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' AS created_at,
        TO_CHAR(observed_at, 'YYYYMMDDHH24MI')::BIGINT AS time_key
    FROM stg_weather
)

SELECT * FROM fact
