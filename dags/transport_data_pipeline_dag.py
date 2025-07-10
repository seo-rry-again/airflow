import logging

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator


import datetime
import pendulum
import json
import io
import pandas as pd


log = logging.getLogger(__name__)
BUCKET_NAME = Variable.get("BUCKET_NAME")
S3_PREFIX = Variable.get("S3_PREFIX")
REDSHIFT_IAM_ROLE_ARN = Variable.get("REDSHIFT_IAM_ROLE_ARN")
DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR")

TARGET_TABLE_BUS = "source.source_bus"
TARGET_TABLE_SUBWAY = "source.source_subway"
REDSHIFT_CONN_ID = "redshift_dev_db"


# 필수 변수 검증
required_vars = [BUCKET_NAME, S3_PREFIX, REDSHIFT_IAM_ROLE_ARN, DBT_PROJECT_DIR]
if not all(required_vars):
    raise ValueError("필수 Airflow Variables가 설정되지 않았습니다.")

log = logging.getLogger(__name__)


# -------------------
# Helper functions
# -------------------
def enforce_schema(df):
    schema_map = {
        "area_code": "string",
        "area_name": "string",
        "total_geton_population_min": "int32",
        "total_geton_population_max": "int32",
        "total_getoff_population_min": "int32",
        "total_getoff_population_max": "int32",
        "geton_30min_population_min": "int32",
        "geton_30min_population_max": "int32",
        "getoff_30min_population_min": "int32",
        "getoff_30min_population_max": "int32",
        "geton_10min_population_min": "int32",
        "geton_10min_population_max": "int32",
        "getoff_10min_population_min": "int32",
        "getoff_10min_population_max": "int32",
        "geton_5min_population_min": "int32",
        "geton_5min_population_max": "int32",
        "getoff_5min_population_min": "int32",
        "getoff_5min_population_max": "int32",
        "station_count": "int32",
        "station_count_basis_month": "datetime64[s]",
        "created_at": "datetime64[s]",
        "observed_at": "datetime64[s]",
    }
    for col, dtype in schema_map.items():
        if col in df.columns:
            if dtype == "int32":
                df[col] = df[col].fillna(0).astype(dtype)
                continue
            df[col] = df[col].fillna(0).astype(dtype)
    return df


def transform_json_to_df(json_list, TRANSPORT_TYPE):
    df = pd.DataFrame(json_list)

    rename_map = {
        f"{TRANSPORT_TYPE}_ACML_GTON_PPLTN_MIN": "total_geton_population_min",
        f"{TRANSPORT_TYPE}_ACML_GTON_PPLTN_MAX": "total_geton_population_max",
        f"{TRANSPORT_TYPE}_ACML_GTOFF_PPLTN_MIN": "total_getoff_population_min",
        f"{TRANSPORT_TYPE}_ACML_GTOFF_PPLTN_MAX": "total_getoff_population_max",
        f"{TRANSPORT_TYPE}_30WTHN_GTON_PPLTN_MIN": "geton_30min_population_min",
        f"{TRANSPORT_TYPE}_30WTHN_GTON_PPLTN_MAX": "geton_30min_population_max",
        f"{TRANSPORT_TYPE}_30WTHN_GTOFF_PPLTN_MIN": "getoff_30min_population_min",
        f"{TRANSPORT_TYPE}_30WTHN_GTOFF_PPLTN_MAX": "getoff_30min_population_max",
        f"{TRANSPORT_TYPE}_10WTHN_GTON_PPLTN_MIN": "geton_10min_population_min",
        f"{TRANSPORT_TYPE}_10WTHN_GTON_PPLTN_MAX": "geton_10min_population_max",
        f"{TRANSPORT_TYPE}_10WTHN_GTOFF_PPLTN_MIN": "getoff_10min_population_min",
        f"{TRANSPORT_TYPE}_10WTHN_GTOFF_PPLTN_MAX": "getoff_10min_population_max",
        f"{TRANSPORT_TYPE}_5WTHN_GTON_PPLTN_MIN": "geton_5min_population_min",
        f"{TRANSPORT_TYPE}_5WTHN_GTON_PPLTN_MAX": "geton_5min_population_max",
        f"{TRANSPORT_TYPE}_5WTHN_GTOFF_PPLTN_MIN": "getoff_5min_population_min",
        f"{TRANSPORT_TYPE}_5WTHN_GTOFF_PPLTN_MAX": "getoff_5min_population_max",
        f"{TRANSPORT_TYPE}_STN_CNT": "station_count",
        f"{TRANSPORT_TYPE}_STN_TIME": "station_count_basis_month",
    }
    df = df.rename(columns=rename_map)
    df["station_count_basis_month"] = pd.to_datetime(
        df["station_count_basis_month"], format="%Y%m%d"
    ).dt.date
    df["created_at"] = pendulum.now("Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")
    df["area_code"] = df["area_code"].fillna("").astype(str).str.strip()

    order_columns = [
        "area_code",
        "area_name",
        "total_geton_population_min",
        "total_geton_population_max",
        "total_getoff_population_min",
        "total_getoff_population_max",
        "geton_30min_population_min",
        "geton_30min_population_max",
        "getoff_30min_population_min",
        "getoff_30min_population_max",
        "geton_10min_population_min",
        "geton_10min_population_max",
        "getoff_10min_population_min",
        "getoff_10min_population_max",
        "geton_5min_population_min",
        "geton_5min_population_max",
        "getoff_5min_population_min",
        "getoff_5min_population_max",
        "station_count",
        "station_count_basis_month",
        "created_at",
        "observed_at",
    ]
    df = df[order_columns]
    return df


# -------------------
# DAG & Tasks
# -------------------
kst = pendulum.timezone("Asia/Seoul")


@dag(
    dag_id="dag_transport",
    schedule="@once",
    # schedule="*/5 * * * *",
    start_date=pendulum.datetime(2025, 7, 8, tz="Asia/Seoul"),
    catchup=False,
    tags=["s3", "parquet"],
)
def transport_data_pipeline():
    @task
    def extract_and_transform(**context):
        hook = S3Hook(aws_conn_id="aws_default")
        s3_client = hook.get_conn()

        process_time = context["data_interval_end"].in_timezone("Asia/Seoul")
        process_time = pendulum.datetime(2025, 7, 8, 0, 5, tz=kst)
        start_time = process_time.subtract(minutes=5)

        log.info(f"🔔{start_time} ~ {process_time} 사이의 raw_json 처리를 시작합니다.")

        date_str = start_time.strftime("%Y%m%d")
        json_files = []

        for min_offset in range(5):
            time_str = (start_time.add(minutes=min_offset)).strftime("%H%M")
            prefix = f"{S3_PREFIX}/{date_str}/{time_str}"
            log.info(prefix)

            response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
            if "Contents" not in response:
                continue
            for obj in response["Contents"]:
                json_files.append(obj["Key"])

        if not json_files:
            log.info("⚠️ No JSON files in last 5 minutes.")
            return

        subway_json_list = []
        bus_json_list = []

        for key in json_files:
            obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
            data = json.load(obj["Body"])

            parts = key.split("/")
            date_part = parts[1]
            time_part = parts[2][:4]
            observed_at = datetime.datetime.strptime(
                date_part + time_part, "%Y%m%d%H%M"
            ).strftime("%Y-%m-%d %H:%M:%S")

            subway = data["LIVE_SUB_PPLTN"]
            subway["area_name"] = data["AREA_NM"]
            subway["area_code"] = data["AREA_CD"]
            subway["observed_at"] = observed_at

            bus = data["LIVE_BUS_PPLTN"]
            bus["area_name"] = data["AREA_NM"]
            bus["area_code"] = data["AREA_CD"]
            bus["observed_at"] = observed_at

            subway_json_list.append(subway)
            bus_json_list.append(bus)

        # Transform
        subway_df = transform_json_to_df(subway_json_list, "SUB")
        bus_df = transform_json_to_df(bus_json_list, "BUS")
        log.info("✅ json to df transformed")

        # Enforce schema
        subway_df = enforce_schema(subway_df)
        bus_df = enforce_schema(bus_df)
        log.info("✅ schema enforced")

        def upload(df, transport_type, time):
            buffer = io.BytesIO()
            df.to_parquet(buffer, engine="pyarrow", index=False)
            buffer.seek(0)

            process_time = time
            date_ymd = process_time.strftime("%Y%m%d")
            time_hm = process_time.strftime("%H%M")
            s3_key = f"processed-data/transport/{transport_type}/{date_ymd}/{time_hm}.parquet"

            s3_client.upload_fileobj(buffer, BUCKET_NAME, s3_key)
            log.info(f"✅ Uploaded: s3://{BUCKET_NAME}/{s3_key}")

        upload(subway_df, "subway", process_time)
        upload(bus_df, "bus", process_time)

        return

    @task
    def load_to_redshift(**context):
        process_time = context["data_interval_end"].in_timezone("Asia/Seoul")
        process_time = pendulum.datetime(2025, 7, 8, 0, 5, tz=kst)

        date_ymd = process_time.strftime("%Y%m%d")
        time_hm = process_time.strftime("%H%M")

        S3_BUS_PATH = f"s3://de6-team1-bucket/processed-data/transport/bus/{date_ymd}/{time_hm}.parquet"
        S3_SUBWAY_PATH = f"s3://de6-team1-bucket/processed-data/transport/subway/{date_ymd}/{time_hm}.parquet"

        log.info(f"Transferring parquet in {S3_BUS_PATH}")
        log.info(f"Transferring parquet in {S3_SUBWAY_PATH}")

        # Redshift Hook 준비
        hook = PostgresHook(postgres_conn_id="redshift_dev_db")

        sql_bus = f"""
            COPY {TARGET_TABLE_BUS}
            (
                area_code, area_name, total_geton_population_min, total_geton_population_max,
                total_getoff_population_min, total_getoff_population_max,
                geton_30min_population_min, geton_30min_population_max,
                getoff_30min_population_min, getoff_30min_population_max,
                geton_10min_population_min, geton_10min_population_max,
                getoff_10min_population_min, getoff_10min_population_max,
                geton_5min_population_min, geton_5min_population_max,
                getoff_5min_population_min, getoff_5min_population_max,
                station_count, station_count_basis_month,
                created_at, observed_at
            )
            FROM '{S3_BUS_PATH}'
            IAM_ROLE '{REDSHIFT_IAM_ROLE_ARN}'
            FORMAT AS PARQUET;
        """

        sql_subway = f"""
            COPY {TARGET_TABLE_SUBWAY}
            (
                area_code, area_name, total_geton_population_min, total_geton_population_max,
                total_getoff_population_min, total_getoff_population_max,
                geton_30min_population_min, geton_30min_population_max,
                getoff_30min_population_min, getoff_30min_population_max,
                geton_10min_population_min, geton_10min_population_max,
                getoff_10min_population_min, getoff_10min_population_max,
                geton_5min_population_min, geton_5min_population_max,
                getoff_5min_population_min, getoff_5min_population_max,
                station_count, station_count_basis_month,
                created_at, observed_at
            )
            FROM '{S3_SUBWAY_PATH}'
            IAM_ROLE '{REDSHIFT_IAM_ROLE_ARN}'
            FORMAT AS PARQUET;
        """

        # SQL 실행
        hook.run(sql_bus)
        hook.run(sql_subway)

        print("✅ Redshift COPY 완료!")

    run_dbt = BashOperator(
        task_id="run_dbt",
        # bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --select fact_transport",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --select fact_transport",
    )

    extract_and_transform() >> load_to_redshift() >> run_dbt


dag_instance = transport_data_pipeline()
