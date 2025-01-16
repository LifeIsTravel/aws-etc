import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

## @params: [folder_path]
args = getResolvedOptions(sys.argv, ['folder_path'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# 버킷 이름, 폴더 경로
bucket_name = 'team5-s3'
folder_path = args['folder_path']
print(f"Folder Path: {folder_path}")

# S3 클라이언트 생성
s3_client = boto3.client('s3')

# S3에서 파일 목록 가져오기
print(f"S3 버킷, 객체: {bucket_name}, folder_path: {folder_path}")
response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)

# "empty"가 포함되지 않은 .parquet 파일 필터링
json_files = [
    f"s3://{bucket_name}/{content['Key']}"
    for content in response.get('Contents', [])
    if content['Key'].endswith('.json') and "empty" not in content['Key']
]

# 파일 목록 출력
print(f"Filtered Json Files: {json_files[0]} ...")

# ==================================================================================================================================================

from pyspark.sql.types import *

schedules_schema = ArrayType(
    MapType(
        StringType(),
        StructType([
            StructField("id", StringType(), True),
            StructField("detail", ArrayType(
                StructType([
                    StructField("sa", StringType(), True),
                    StructField("ea", StringType(), True),
                    StructField("av", StringType(), True),
                    StructField("fn", StringType(), True),
                    StructField("sdt", StringType(), True),
                    StructField("edt", StringType(), True),
                    StructField("oav", StringType(), True),
                    StructField("jt", StringType(), True),
                    StructField("ft", StringType(), True),
                    StructField("ct", StringType(), True),
                    StructField("et", StringType(), True),
                    StructField("im", BooleanType(), True),
                    StructField("carbonEmission", DoubleType(), True)
                ])
            ), True),
            StructField("journeyTime", ArrayType(StringType()), True)
        ])
    )
)

passenger_schema = StructType([
    StructField("Fare", StringType(), True),
    StructField("NaverFare", StringType(), True),
    StructField("Tax", StringType(), True),
    StructField("QCharge", StringType(), True),
    StructField("MemberFares", ArrayType(
        StructType([
            StructField("Fare", StringType(), True),
            StructField("NaverFare", StringType(), True),
            StructField("Tax", StringType(), True),
            StructField("QCharge", StringType(), True),
            StructField("Grade", StringType(), True),
            StructField("ReserveParameter", StructType([
                StructField("#cdata-section", StringType(), True)
            ]), True)
        ]), True)
                )
])

fares_schema = MapType(
    StringType(),
    StructType([
        StructField("sch", ArrayType(StringType()), True),
        StructField("fare", MapType(
            StringType(),
            ArrayType(
                StructType([
                    StructField("FareType", StringType(), True),
                    StructField("AgtCode", StringType(), True),
                    StructField("ConfirmType", StringType(), True),
                    StructField("BaggageType", StringType(), True),
                    StructField("ReserveParameter", StructType([
                        StructField("#cdata-section", StringType(), True)
                    ]), True),
                    StructField("PromotionParameter", StructType([
                        StructField("#cdata-section", StringType(), True)
                    ]), True),
                    StructField("Adult", passenger_schema, True),
                    StructField("Child", passenger_schema, True),
                    StructField("Infant", passenger_schema, True)
                ])
            )
        ), True)
    ])
)

main_schema = StructType([
    StructField("data", StructType([
        StructField("internationalList", StructType([
            StructField("galileoKey", StringType(), True),
            StructField("galileoFlag", BooleanType(), True),
            StructField("travelBizKey", StringType(), True),
            StructField("travelBizFlag", BooleanType(), True),
            StructField("totalResCnt", IntegerType(), True),
            StructField("resCnt", IntegerType(), True),
            StructField("results", StructType([
                StructField("airlines", MapType(StringType(), StringType()), True),
                StructField("airports", MapType(StringType(), StringType()), True),
                StructField("fareTypes", MapType(StringType(), StringType()), True),
                StructField("errors", ArrayType(StringType()), True),
                StructField("carbonEmissionAverage", StructType([
                    StructField("directFlightCarbonEmissionItineraryAverage", MapType(StringType(), DoubleType()),
                                True),
                    StructField("directFlightCarbonEmissionAverage", DoubleType(), True)
                ]), True),
                StructField("schedules", schedules_schema, True),
                StructField("fares", fares_schema, True)
            ]), True)
        ]), True)
    ]), True)
])

# 파일이 존재할 경우에만 읽기
if json_files:
    df = spark.read.schema(main_schema).option("multiline", "true").option("mode", "PERMISSIVE").json(json_files)
    df.show(3)
    print("파일 존재")
else:
    print("유효한 json 파일 없음")

# ==================================================================================================================================================
# schedule 읽기

from pyspark.sql.functions import explode, col, map_values

df_schedules = (
    df
    .select(explode(col("data.internationalList.results.schedules")).alias("schedule"))
    .select(explode(map_values(col('schedule'))).alias('schedule_value'))
    .select(
        col("schedule_value.id").alias("schedule_id"),
        col("schedule_value.detail").alias("detail")
    )
    .select(
        "schedule_id",
        explode(col("detail")).alias("detail")
    )
    .select(
        "schedule_id",
        col("detail.sa").alias("departure_display_code"),
        col("detail.ea").alias("arrival_display_code"),
        col("detail.av").alias("carrier_company"),
        col("detail.sdt").alias("departure_time"),
        col("detail.edt").alias("arrival_time"),
    )
)

# ==================================================================================================================================================
# fares 읽기

from pyspark.sql.functions import explode, col, coalesce, lit, min as min_, row_number, substring
from pyspark.sql.window import Window

df_fares = (
    df.select(
        explode(col("data.internationalList.results.fares")).alias("fare_key", "fare_value")
    )
    .select(
        col("fare_key"),
        col("fare_value.fare.A01").alias("A01_fare")
    )
    .where(col("A01_fare").isNotNull())
    .withColumn("fare_detail", explode(col("A01_fare")))
    .select(
        col("fare_key"),
        col("fare_detail.Adult.Fare").alias("Fare"),
        col("fare_detail.Adult.NaverFare").alias("NaverFare"),
        col("fare_detail.Adult.Tax").alias("Tax"),
        col("fare_detail.Adult.QCharge").alias("QCharge"),
        col("fare_detail.ReserveParameter.`#cdata-section`").alias("url")
    )
    .select(
        col("fare_key"),
        coalesce(col("Fare").cast("double"), lit(0.0)).alias("Fare"),
        coalesce(col("NaverFare").cast("double"), lit(0.0)).alias("NaverFare"),
        coalesce(col("Tax").cast("double"), lit(0.0)).alias("Tax"),
        coalesce(col("QCharge").cast("double"), lit(0.0)).alias("QCharge"),
        col("url")
    )
    .withColumn(
        "amount",
        col("Fare") + col("NaverFare") + col("Tax") + col("QCharge")
    )
)

windowSpec = Window.partitionBy("fare_key").orderBy(col("amount").asc())

df_min_fares = (
    df_fares
    .withColumn("row_number", row_number().over(windowSpec))
    .filter(col("row_number") == 1)
    .select("fare_key", "amount", "url")
)

# ==================================================================================================================================================
# schedule, fares 합치기

df_combined = (
    df_schedules.join(
        df_min_fares,
        df_schedules.schedule_id == df_min_fares.fare_key,
        how="inner"
    )
    .select(
        df_schedules.schedule_id.alias("schedule_id"),
        "departure_display_code",
        "arrival_display_code",
        "carrier_company",
        substring(col("departure_time"), 3, 10).alias("departure_time"),
        substring(col("arrival_time"), 3, 10).alias("arrival_time"),
        substring(col("departure_time"), 3, 6).alias("departure_date"),
        col("amount"),
        col("url")
    )
)

# ==================================================================================================================================================
# airports, airlines 구분위해 df 생성

df_airports = df.select(explode("data.internationalList.results.airports").alias("airport_code", "airport_name"))
df_airports = df_airports.dropDuplicates()

df_airlines = df.select(explode("data.internationalList.results.airlines").alias("airline_code", "airline_name"))
df_airlines = df_airlines.dropDuplicates()

# ==================================================================================================================================================
# airport 추출

from pyspark.sql.functions import lit
from datetime import datetime

df_combined = df_combined.alias('dfc') \
    .join(
    df_airports.withColumnRenamed('airport_code', 'departure_display_code') \
        .withColumnRenamed('airport_name', 'departure_name') \
        .alias('dfa'),
    on='departure_display_code',
    how='left'
)

df_combined = df_combined.alias('dfc') \
    .join(
    df_airports.withColumnRenamed('airport_code', 'arrival_display_code') \
        .withColumnRenamed('airport_name', 'arrival_name') \
        .alias('dfa'),
    on='arrival_display_code',
    how='left'
)

df_combined = df_combined.withColumn('stop_count', lit(0))

# agent_name 및 last_updated 컬럼을 일단 '-'로 설정
df_combined = df_combined.withColumn('agent_name', lit('-'))
df_combined = df_combined.withColumn('last_updated', lit('-'))

# ==================================================================================================================================================
# airline 추출

from pyspark.sql.functions import create_map, lit, col, array, transform
from itertools import chain

mapping_expr = create_map([lit(x) for x in chain(*df_airlines.select("airline_code", "airline_name").collect())])

# 만약 carrier_company가 이미 배열이면 이 단계는 생략 가능
df_combined = df_combined.withColumn("carrier_company_array", array("carrier_company"))

df_combined = df_combined.withColumn(
    "carrier_names",
    transform(col("carrier_company_array"), lambda x: mapping_expr.getItem(x))
)

df_combined = df_combined.drop("carrier_company_array")

# ==================================================================================================================================================
# 추출 날짜, 시간 저장하기

from pyspark.sql.functions import lit

# 슬래시 기준으로 경로를 나누기
parts = folder_path.split("/")

# 연도, 월, 일, 시간, 분을 추출
year = parts[2]
month = parts[3]
day = parts[4]
save_time = parts[5]
time = parts[5].replace("-", "")  # "03-04" -> "0304"

# 원하는 포맷으로 합치기
formatted_str = year + month + day + time

df_combined = df_combined.withColumn("extracted_at", lit(formatted_str))

# ==================================================================================================================================================
# 순서 맞추기

df_result = df_combined.select(
    "extracted_at",
    "departure_date",
    "departure_display_code",
    "departure_name",
    "arrival_display_code",
    "arrival_name",
    "carrier_names",
    "departure_time",
    "arrival_time",
    "agent_name",
    "amount",
    "url",
    "last_updated",
    "stop_count",
)
# ==================================================================================================================================================
# S3에 parquet로 저장

save_path = f"s3://{bucket_name}/transform_data/flights/{year}/{month}/{day}/{save_time}"
df_result.write.parquet(save_path, mode='append')

# RDS를 위해 CSV 파일도 일단 저장
from pyspark.sql.functions import array_join

df_result = df_result.withColumn("carrier_names", array_join("carrier_names", ","))
df_result.write.option("header", "true").csv(save_path, mode='append')

# ==================================================================================================================================================

print()
print("transform 완료된 df")
df_result.show(3)

# SparkContext 정리
sc.stop()

print("ETL 작업이 성공적으로 완료되었습니다.")
