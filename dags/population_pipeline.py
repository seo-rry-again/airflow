import json
import logging
import pandas as pd
import pendulum
import botocore.exceptions
import io
import textwrap

from typing import List
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.operators.bash import BashOperator

logger = logging.getLogger()
S3_BUCKET_NAME = "de6-team1-bucket"
DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = Variable.get("DBT_PROFILES_DIR")
REDSHIFT_IAM_ROLE = Variable.get("REDSHIFT_IAM_ROLE")


def get_s3_client(conn_id="aws_conn_id"):
    """airflow에 등록된 aws_conn_id를 사용하여 S3 클라이언트 반환"""
    s3_hook = S3Hook(aws_conn_id=conn_id)
    return s3_hook.get_conn()


def read_from_s3(bucket_name: str, key: str, conn_id="aws_conn_id"):
    """지정한 S3 버킷과 키에 해당하는 객체를 문자열로 반환"""
    s3 = get_s3_client(conn_id)
    response = s3.get_object(Bucket=bucket_name, Key=key)
    return response["Body"].read().decode("utf-8")


"""처리된 observed_at 값이 기록된 s3_key(population.json)에 processed_history_data 업데이트"""
def upload_processed_history_to_s3(
    s3_client,
    bucket_name: str,
    s3_key: str,
    processed_history_data: dict,  # area_code 별로 처리된 observed_at 값 목록을 담은 딕셔너리
):
    updated_content_json_string = json.dumps(
        processed_history_data, indent=4, ensure_ascii=False
    )

    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=updated_content_json_string.encode("utf-8"),
        ContentType="application/json",
    )

    total_records_count = sum(
        len(observations) for observations in processed_history_data.values()
    )
    logger.info(
        f"✅ S3에 {total_records_count}개의 처리 이력을 성공적으로 업데이트했습니다: s3://{bucket_name}/{s3_key}"
    )


@dag(
    dag_id="dag_population",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2025, 7, 3, 0, 5, tz="Asia/Seoul"),
    docs_md=textwrap.dedent("""
        - **추출 및 변환**: S3에서 raw json 데이터를 추출, 변환하고 중복 처리
        - **Parquet 업로드**: 처리된 데이터는 Parquet 파일로 S3에 업로드
        - **Redshift 적재**: S3의 Parquet 파일을 Redshift 테이블에 적재
    """),
    catchup=False,
    tags=["population", "silver", "ETL"],
    default_args={"owner": "hyeonuk"},
)
def population_data_pipeline():
    @task
    def extract_and_transform(**context) -> dict:
        """
        S3에 최근 5분 동안 수집된 raw json 데이터에서 인구 데이터 추출
        - ex) logical_date가 20:10이면 20:05~09 사이에 수집된 raw json 탐색
        """
        logical_date = context["logical_date"]

        process_start_time = logical_date

        s3 = get_s3_client()
        files_to_process = []

        # 5분 내의 생성된 데이터 prefix 생성 후 실제 존재하는 것만 가져옴
        for i in range(5):
            file_time = process_start_time.subtract(minutes=(5 - i))

            s3_prefix_date_path = file_time.strftime("%Y%m%d")
            s3_prefix_time_name = file_time.strftime("%H%M")
            full_s3_prefix = f"raw-json/{s3_prefix_date_path}/{s3_prefix_time_name}_"

            response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=full_s3_prefix)

            for obj in response.get("Contents", []):
                file_key = obj["Key"]

                try:
                    raw_json = read_from_s3(S3_BUCKET_NAME, file_key)
                    raw_data = json.loads(raw_json)

                    citydata = raw_data.get("LIVE_PPLTN_STTS", [])
                    if not citydata:
                        continue

                    observed_at = (
                        citydata[0]
                        .get("PPLTN_TIME", "unknown_time")
                        .replace(":", "")
                        .replace(" ", "")
                    )

                    area_id = int(file_key.split("_")[-1].split(".")[0])

                    files_to_process.append(
                        {
                            "key": file_key,
                            "observed_at": observed_at,
                            "data": citydata,
                            "area_id": area_id,
                            "file_time": file_time,
                            "file_name": f"{s3_prefix_time_name}_{area_id}.json",
                        }
                    )

                    # logger.info(
                    #     f"[DEBUG] Found file: {file_key}, observed_at={observed_at}"
                    # )

                except botocore.exceptions.ClientError as e:
                    if e.response["Error"]["Code"] == "NoSuchKey":
                        logger.warning(f"S3 key not found during read: {file_key}")
                        continue
                    else:
                        raise

        processed_history_s3_key = (
            "processed_history/population.json"  # 처리 내역이 저장되는 키
        )
        processed_observed_at_set = set()
        processed_observed_at_dict = {}

        try:
            response = s3.get_object(  # 처리된 observed_at인지 판단하기 위해 population.json 로드
                Bucket=S3_BUCKET_NAME, Key=processed_history_s3_key
            )
            processed_observed_at_dict = json.loads(response["Body"].read())

            for area_id_str, values in processed_observed_at_dict.items():
                area_id_int = int(area_id_str)
                for value in values:
                    observed_at = value["observed_at"]
                    processed_observed_at_set.add((area_id_int, observed_at))
            logger.info(
                f"🔔 S3에서 {len(processed_observed_at_set)}개의 기존 처리 이력을 로드했습니다."
            )

        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.warning(
                    f"🚨 {processed_history_s3_key} 경로에 기존 처리 이력 파일이 없습니다. 새로 시작합니다."
                )
            else:
                raise

        # 이전에 처리된 observed_at인지 판단
        def is_processed(area_id: int, observed_at: str) -> bool:
            return (area_id, observed_at) in processed_observed_at_set

        # population 데이터 리스트
        source_population_data = []

        for file_info in files_to_process:
            file_time = file_info["file_time"]
            area_id = file_info["area_id"]
            file_name = file_info["file_name"]

            key = f"raw-json/{file_time.strftime('%Y%m%d')}/{file_name}"

            try:
                response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
                content = response["Body"].read()
                raw_data = json.loads(content)
                raw_population_data = raw_data["LIVE_PPLTN_STTS"][
                    0
                ]  # ✅ population은 배열 첫 번째 값
                raw_observed_at = raw_population_data.get("PPLTN_TIME")

                try:
                    observed_at = pendulum.from_format(
                        raw_observed_at, "YYYY-MM-DD HH:mm", tz="Asia/Seoul"
                    ).format("YYYY-MM-DD HH:mm:ss")
                except Exception as e:
                    logger.error(
                        f"🚨 해당 시각({raw_observed_at}) 파싱에 실패했습니다. 오류: {e}"
                    )
                    continue

                # 처리된 이력이 있으면 스킵 없으면 추가
                if is_processed(area_id=area_id, observed_at=observed_at):
                    logger.info(
                        f"⏭️ 이미 처리된 데이터 스킵: {file_name} (area_id: {area_id}, observed_at: {observed_at})"
                    )
                    continue

                source_population_data.append(
                    {
                        "area_code": str(raw_population_data.get("AREA_CD", "")),
                        "area_name": str(raw_population_data.get("AREA_NM", "")),
                        "congestion_label": str(
                            raw_population_data.get("AREA_CONGEST_LVL", "")
                        ),
                        "congestion_message": str(
                            raw_population_data.get("AREA_CONGEST_MSG", "")
                        ),
                        "population_min": int(
                            float(raw_population_data.get("AREA_PPLTN_MIN"))
                        ),
                        "population_max": int(
                            float(raw_population_data.get("AREA_PPLTN_MAX"))
                        ),
                        "male_population_ratio": float(
                            raw_population_data.get("MALE_PPLTN_RATE")
                        ),
                        "female_population_ratio": float(
                            raw_population_data.get("FEMALE_PPLTN_RATE")
                        ),
                        "age_0s_ratio": float(raw_population_data.get("PPLTN_RATE_0")),
                        "age_10s_ratio": float(
                            raw_population_data.get("PPLTN_RATE_10")
                        ),
                        "age_20s_ratio": float(
                            raw_population_data.get("PPLTN_RATE_20")
                        ),
                        "age_30s_ratio": float(
                            raw_population_data.get("PPLTN_RATE_30")
                        ),
                        "age_40s_ratio": float(
                            raw_population_data.get("PPLTN_RATE_40")
                        ),
                        "age_50s_ratio": float(
                            raw_population_data.get("PPLTN_RATE_50")
                        ),
                        "age_60s_ratio": float(
                            raw_population_data.get("PPLTN_RATE_60")
                        ),
                        "age_70s_ratio": float(
                            raw_population_data.get("PPLTN_RATE_70")
                        ),
                        "resident_ratio": float(
                            raw_population_data.get("RESNT_PPLTN_RATE")
                        ),
                        "non_resident_ratio": float(
                            raw_population_data.get("NON_RESNT_PPLTN_RATE")
                        ),
                        "is_replaced": str(raw_population_data.get("REPLACE_YN"))
                        == "Y",
                        "observed_at": observed_at,
                        "created_at": pendulum.now("Asia/Seoul").to_datetime_string(),
                    }
                )

                # 처리 이력(oberved_at) 추가
                processed_observed_at_set.add((area_id, observed_at))
                if str(area_id) not in processed_observed_at_dict:
                    processed_observed_at_dict[str(area_id)] = []
                processed_observed_at_dict[str(area_id)].append(
                    {
                        "observed_at": observed_at,
                        "processed_at": pendulum.now("Asia/Seoul").to_datetime_string(),
                    }
                )

                # logger.info(f"✅ {file_name} 변환 완료")

            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    logger.info(f"🚨 파일이 없음. 건너뜀: {key}")
                    continue
                else:
                    raise

        return {
            "source_population_data": source_population_data,
            "processed_observed_at_dict": processed_observed_at_dict,
        }

    @task
    def load_to_s3(result: dict):
        """
        이전 task에서 처리된 이력이 없는 데이터들은 Parquet으로 변환
        """
        source_population_data = result.get("source_population_data", [])

        if not source_population_data:
            logger.info("no population data")
            return ""

        df = pd.DataFrame(source_population_data)

        columns_order = [
            "area_name",
            "area_code",
            "congestion_label",
            "congestion_message",
            "population_min",
            "population_max",
            "male_population_ratio",
            "female_population_ratio",
            "age_0s_ratio",
            "age_10s_ratio",
            "age_20s_ratio",
            "age_30s_ratio",
            "age_40s_ratio",
            "age_50s_ratio",
            "age_60s_ratio",
            "age_70s_ratio",
            "resident_ratio",
            "non_resident_ratio",
            "is_replaced",
            "observed_at",
            "created_at",
        ]

        df = df[columns_order]
        df = df.astype(
            {
                "area_code": "string",
                "area_name": "string",
                "congestion_label": "string",
                "congestion_message": "string",
                "population_min": "Int32",
                "population_max": "Int32",
                "male_population_ratio": "float32",
                "female_population_ratio": "float32",
                "age_0s_ratio": "float32",
                "age_10s_ratio": "float32",
                "age_20s_ratio": "float32",
                "age_30s_ratio": "float32",
                "age_40s_ratio": "float32",
                "age_50s_ratio": "float32",
                "age_60s_ratio": "float32",
                "age_70s_ratio": "float32",
                "resident_ratio": "float32",
                "non_resident_ratio": "float32",
                "is_replaced": "bool",
                "observed_at": "datetime64[ns]",
                "created_at": "datetime64[ns]",
            }
        )
        logger.info(f"population 데이터 변환 완료. rows: {len(df)}")

        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        # 변환된 Parquet들을 S3에 저장
        s3 = get_s3_client()
        merged_key = f"processed-data/population/{pendulum.now('Asia/Seoul').format('YYYYMMDD_HHmm')}.parquet"

        s3.put_object(Bucket=S3_BUCKET_NAME, Key=merged_key, Body=buffer.getvalue())

        logger.info(
            f"✅ population parquet 파일을 저장했습니다: s3://{S3_BUCKET_NAME}/{merged_key}"
        )

        # 처리 어력 업데이트 후 S3에 덮어쓰기
        processed_history_s3_key = "processed_history/population.json"
        try:
            upload_processed_history_to_s3(
                s3_client=s3,
                bucket_name=S3_BUCKET_NAME,
                s3_key=processed_history_s3_key,
                processed_history_data=result["processed_observed_at_dict"],
            )
        except Exception as e:
            logger.info(f"❌ 최종 처리 이력 업로드 중 오류 발생: {e}")
            raise

        return merged_key

    @task
    def load_to_redshift(merged_key: List[str]):
        if not merged_key:
            """
            5분 사이 수집된 모든 json이 이미 처리됐으면 parquet으로 변환할 것이 없으므로 merged_key는 빈 리스트임
            그러면 redshift 적재 시 copy_sql의 FROM 부분이 's3://{S3_BUCKET_NAME}'가 되므로 버킷의 모든 데이터를 redshift에 적재하려는 오류가 발생하므로 그 상황에 대비한 처리
            """
            logger.info(
                "[load_to_redshift] 처리할 parquet 파일이 없어 적재를 스킵합니다."
            )
            return "SKIPPED"

        hook = RedshiftSQLHook(redshift_conn_id="redshift_dev_db")
        source_table = "source.source_population"

        copy_sql = f"""
            COPY {source_table}
            FROM 's3://{S3_BUCKET_NAME}/{merged_key}'
            IAM_ROLE '{REDSHIFT_IAM_ROLE}'
            FORMAT PARQUET;
        """

        hook.run(copy_sql)

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps && dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --select fact_population",
    )

    # DAG 순서 명시
    extract = extract_and_transform()
    merge = load_to_s3(extract)
    load_to_redshift(merge) >> run_dbt


dag_instance = population_data_pipeline()
