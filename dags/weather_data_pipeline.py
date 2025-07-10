import logging
from typing import Optional
import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.models.variable import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import json


logger = logging.getLogger()
S3_BUCKET = Variable.get("BUCKET_NAME")
SOURCE_PREFIX = Variable.get("S3_SOURCE_PREFIX")
EXTRACTED_PREFIX = Variable.get("S3_WEATHER_EXTRACTED_PREFIX")
PROCESSED_PREFIX = Variable.get("S3_WEATHER_PROCESSED_PREFIX")
IAM_ROLE = Variable.get("REDSHIFT_IAM_ROLE_ARN")
REDSHIFT_TABLE = "source.source_weather"
DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = Variable.get("DBT_PROFILES_DIR")


def _parse_int(value) -> Optional[int]:
    """안전한 정수 변환 (실패 시 None 반환)"""
    try:
        return int(value)
    except (ValueError, TypeError) as e:
        logger.warning(
            f"Failed to convert to int: {value!r} ({type(value).__name__}) - {e}"
        )
        return None


def _parse_float(value) -> Optional[float]:
    """안전한 실수 변환 (실패 시 None 반환)"""
    try:
        return float(value)
    except (ValueError, TypeError) as e:
        logger.warning(
            f"Failed to convert to float: {value!r} ({type(value).__name__}) - {e}"
        )
        return None


def _parse_file_timestamp(key: str) -> pendulum.DateTime:
    """
    S3 키에서 타임스탬프 추출 (형식: YYYYMMDDHHmm_...)
    예: 'raw-json/20250705/2301_xxx.json' → 2025-07-05 23:01 KST
    """
    parts = key.split("/")
    date_str = parts[-2]
    time_str = parts[-1].split("_")[0]

    # 표준 형식으로 변환 (YYYY-MM-DD HH:mm)
    parsed_str = (
        f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]} {time_str[:2]}:{time_str[2:4]}"
    )
    return pendulum.parse(parsed_str, tz="Asia/Seoul")


@dag(
    dag_id="weather_data_pipeline",
    start_date=pendulum.datetime(2025, 6, 30),
    schedule="*/10 * * * *",
    doc_md="""
    # 기상데이터 ETL 파이프라인
    - **추출 및 변환**: S3에서 원시 JSON 데이터를 추출, 변환하고 이미 처리된 레코드를 필터링
    - **Parquet 업로드**: 처리된 데이터를 Parquet 파일로 S3에 업로드
    - **Redshift 로드**: S3의 Parquet 파일을 Redshift 테이블로 로드
    """,
    catchup=False,
    tags=["weather", "ETL", "silver"],
)
def weather_data_pipeline():
    @task
    def extract(**context) -> str:
        """
        S3에서 10분 간격으로 쌓인 기상 데이터 추출
        - 최근 10분간 생성된 1분 단위 파일 모두 처리
        - 한국 시간대(Asia/Seoul) 기준으로 시간 계산
        """
        utc_time = context["logical_date"]
        kst_time = utc_time.in_timezone("Asia/Seoul")
        start_kst = kst_time.subtract(minutes=10)

        # 10분 내 모든 1분 단위 prefix 생성
        prefixes = [
            f"{SOURCE_PREFIX}/{start_kst.add(minutes=i).format('YYYYMMDD/HHmm')}"
            for i in range(10)
        ]

        # 각 1분 prefix별로 처리
        s3_hook = S3Hook(aws_conn_id="aws_default")
        records = []
        for prefix in prefixes:
            for key in s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=prefix):
                file_time = _parse_file_timestamp(key)
                if start_kst <= file_time < kst_time:
                    data = json.loads(
                        s3_hook.get_key(key, S3_BUCKET)
                        .get()["Body"]
                        .read()
                        .decode("utf-8")
                    )
                    if "WEATHER_STTS" not in data:
                        continue

                    weather = data["WEATHER_STTS"][0]
                    records.append(
                        {
                            "area_code": data["AREA_CD"],
                            "area_name": data["AREA_NM"],
                            "temperature": _parse_float(weather["TEMP"]),
                            "sensible_temperature": _parse_float(
                                weather["SENSIBLE_TEMP"]
                            ),
                            "max_temperature": _parse_float(weather["MAX_TEMP"]),
                            "min_temperature": _parse_float(weather["MIN_TEMP"]),
                            "humidity": _parse_int(weather["HUMIDITY"]),
                            "wind_direction": weather["WIND_DIRCT"],
                            "wind_speed": _parse_float(weather["WIND_SPD"]),
                            "precipitation": weather["PRECIPITATION"],
                            "precipitation_type": weather["PRECPT_TYPE"],
                            "precipitation_message": weather["PCP_MSG"],
                            "sunrise": weather["SUNRISE"],
                            "sunset": weather["SUNSET"],
                            "uv_index_level": _parse_int(weather["UV_INDEX_LVL"]),
                            "uv_index_desc": weather["UV_INDEX"],
                            "uv_message": weather["UV_MSG"],
                            "pm25_index": weather["PM25_INDEX"],
                            "pm25_value": _parse_int(weather["PM25"]),
                            "pm10_index": weather["PM10_INDEX"],
                            "pm10_value": _parse_int(weather["PM10"]),
                            "air_quality_index": weather["AIR_IDX"],
                            "air_quality_value": _parse_float(weather["AIR_IDX_MVL"]),
                            "air_quality_main": weather["AIR_IDX_MAIN"],
                            "air_quality_message": weather["AIR_MSG"],
                            "observed_at": weather["WEATHER_TIME"],
                            "created_at": pendulum.now(
                                "Asia/Seoul"
                            ).to_datetime_string(),
                        }
                    )

        # 추출된 레코드 저장
        output_key = f"{EXTRACTED_PREFIX}/{start_kst.format('YYYYMMDD')}/{start_kst.format('HHmm')}-{kst_time.format('HHmm')}.json"
        s3_hook.load_bytes(
            bytes_data=json.dumps(records).encode("utf-8"),
            key=output_key,
            bucket_name=S3_BUCKET,
            replace=True,
        )
        logger.info(f"Extracted {len(records)} records.Saved to: {output_key}")
        return output_key

    @task
    def transform(extracted_key, **context) -> str:
        """
        추출된 JSON 데이터를 Parquet으로 변환
        - Grouping으로 지역코드+관측시간 기준으로 중복 제거
        - 숫자형 데이터는 최대값 기준 통합
        """
        s3_hook = S3Hook(aws_conn_id="aws_default")
        obj = s3_hook.get_key(key=extracted_key, bucket_name=S3_BUCKET)
        records = json.loads(obj.get()["Body"].read().decode("utf-8"))
        df = pd.DataFrame(records)
        df = df.astype(
            {
                "humidity": "int32",
                "uv_index_level": "int32",
                "pm25_value": "int32",
                "pm10_value": "int32",
                "temperature": "float32",
                "sensible_temperature": "float32",
                "max_temperature": "float32",
                "min_temperature": "float32",
                "wind_speed": "float32",
                "air_quality_value": "float32",
                "observed_at": "datetime64[ns]",
                "created_at": "datetime64[ns]",
            }
        )

        # 데이터 집계
        grouped_df = (
            df.groupby(["area_code", "observed_at"], dropna=False)
            .max(numeric_only=False)
            .reset_index()
        )

        # Parquet 변환
        schema = pa.schema(
            [
                # 정수 타입
                pa.field("humidity", pa.int32()),
                pa.field("uv_index_level", pa.int32()),
                pa.field("pm25_value", pa.int32()),
                pa.field("pm10_value", pa.int32()),
                # 실수 타입 (float4)
                pa.field("temperature", pa.float32()),
                pa.field("sensible_temperature", pa.float32()),
                pa.field("max_temperature", pa.float32()),
                pa.field("min_temperature", pa.float32()),
                pa.field("wind_speed", pa.float32()),
                pa.field("air_quality_value", pa.float32()),
                # 문자열 타입
                pa.field("area_code", pa.string()),
                pa.field("area_name", pa.string()),
                pa.field("wind_direction", pa.string()),
                pa.field("precipitation", pa.string()),
                pa.field("precipitation_type", pa.string()),
                pa.field("precipitation_message", pa.string()),
                pa.field("sunrise", pa.string()),
                pa.field("sunset", pa.string()),
                pa.field("uv_index_desc", pa.string()),
                pa.field("uv_message", pa.string()),
                pa.field("pm25_index", pa.string()),
                pa.field("pm10_index", pa.string()),
                pa.field("air_quality_index", pa.string()),
                pa.field("air_quality_main", pa.string()),
                pa.field("air_quality_message", pa.string()),
                # 날짜/시간 타입
                pa.field("observed_at", pa.timestamp("ms")),
                pa.field("created_at", pa.timestamp("ms")),
            ]
        )
        buffer = BytesIO()
        pq.write_table(pa.Table.from_pandas(grouped_df, schema), buffer)
        buffer.seek(0)

        # 변환된 Parquet를 S3에 저장
        logical_date = context["logical_date"].in_timezone("Asia/Seoul")
        date_dir = logical_date.format("YYYYMMDD")
        filename = f"{logical_date.subtract(minutes=10).format('HHmm')}-{logical_date.format('HHmm')}.parquet"
        processed_key = f"{PROCESSED_PREFIX}/{date_dir}/{filename}"
        s3_hook.load_file_obj(
            file_obj=buffer, key=processed_key, bucket_name=S3_BUCKET, replace=True
        )
        logger.info(f"Transformed data saved to: {processed_key}")
        return processed_key

    @task
    def load_to_redshift(parquet_key, **context) -> None:
        """S3 Parquet 파일을 Redshift에 로드"""
        redshift = RedshiftSQLHook(redshift_conn_id="redshift_conn_id")

        # 시간 범위 계산
        utc_time = context["logical_date"]
        kst_time = utc_time.in_timezone("Asia/Seoul")
        end_kst = kst_time.format("YYYY-MM-DD HH:mm:ss")
        start_kst = kst_time.subtract(minutes=10).format("YYYY-MM-DD HH:mm:ss")

        # Transaction 열고 해당 날짜 데이터 지운 후 COPY 진행
        redshift.run("BEGIN")
        redshift.run(f"""
        DELETE FROM
            {REDSHIFT_TABLE}
        WHERE
            observed_at BETWEEN '{start_kst}' AND '{end_kst}'
        """)
        redshift.run(f"""
        COPY {REDSHIFT_TABLE} (
            humidity, uv_index_level, pm25_value, pm10_value, temperature,
            sensible_temperature, max_temperature, min_temperature, wind_speed,
            air_quality_value, area_code, area_name, wind_direction, precipitation,
            precipitation_type, precipitation_message, sunrise, sunset,
            uv_index_desc, uv_message, pm25_index, pm10_index, air_quality_index,
            air_quality_main, air_quality_message, observed_at, created_at
        )
        FROM 's3://{S3_BUCKET}/{parquet_key}'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS PARQUET
        """)
        redshift.run("COMMIT")
        logger.info(f"Deleted and reloaded data between {start_kst} and {end_kst}")

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --select fact_weather",
    )

    # DAG 실행 흐름
    extracted_key = extract()
    parquet_key = transform(extracted_key)
    load_to_redshift(parquet_key) >> run_dbt


weather_data_pipeline()
