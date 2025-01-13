import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, when, count

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 경로 설정
departure_path = "s3://team5-s3/raw_data/flight_operations/flight_operations_출발_20241223.parquet"
arrival_path = "s3://team5-s3/raw_data/flight_operations/flight_operations_도착_20241223.parquet"

target_path = "s3://team5-s3/transform_data/flight_operations/20241223/"

# 대상 공항 정보 DataFrame 생성
target_airports = spark.createDataFrame([
    ("NRT", "도쿄/나리타", "일본"),
    ("FUK", "후쿠오카", "일본"),
    ("KIX", "간사이", "일본"),
    ("CTS", "삿포로", "일본")
], ["airport_code", "airport_name", "country"])

# 출발/도착 데이터 각각 읽기
departure_df = spark.read.parquet(departure_path)
arrival_df = spark.read.parquet(arrival_path)

# 컬럼명 변경
departure_df = departure_df.withColumnRenamed("출발시간", "실제시간")
arrival_df = arrival_df.withColumnRenamed("도착시간", "실제시간")

departure_df = departure_df.withColumnRenamed("출/도착구분", "operation_type") \
    .withColumnRenamed("날짜", "date") \
    .withColumnRenamed("항공사", "airline") \
    .withColumnRenamed("편명", "flight_number") \
    .withColumnRenamed("출발공항코드", "departure_airport_code") \
    .withColumnRenamed("출발공항명", "departure_airport_name") \
    .withColumnRenamed("도착공항코드", "arrival_airport_code") \
    .withColumnRenamed("도착공항명", "arrival_airport_name") \
    .withColumnRenamed("계획시간", "scheduled_time") \
    .withColumnRenamed("예상시간", "estimated_time") \
    .withColumnRenamed("실제시간", "actual_time") \
    .withColumnRenamed("구분", "category") \
    .withColumnRenamed("현황", "status")
    
arrival_df = arrival_df.withColumnRenamed("출/도착구분", "operation_type") \
    .withColumnRenamed("날짜", "date") \
    .withColumnRenamed("항공사", "airline") \
    .withColumnRenamed("편명", "flight_number") \
    .withColumnRenamed("출발공항코드", "departure_airport_code") \
    .withColumnRenamed("출발공항명", "departure_airport_name") \
    .withColumnRenamed("도착공항코드", "arrival_airport_code") \
    .withColumnRenamed("도착공항명", "arrival_airport_name") \
    .withColumnRenamed("계획시간", "scheduled_time") \
    .withColumnRenamed("예상시간", "estimated_time") \
    .withColumnRenamed("실제시간", "actual_time") \
    .withColumnRenamed("구분", "category") \
    .withColumnRenamed("현황", "status")

# 각각의 데이터 필터링
filtered_departure = departure_df \
    .join(
        target_airports.select("airport_code"),
        col("arrival_airport_code") == target_airports.airport_code,
        "inner"
    ) \
    .filter(col("status").isin(["출발", "지연", "취소"])) \
    .filter(col("category") == "여객") \
    .select(departure_df.columns)

filtered_arrival = arrival_df \
    .join(
        target_airports.select("airport_code"),
        col("departure_airport_code") == target_airports.airport_code,
        "inner"
    ) \
    .filter(col("status").isin(["도착", "지연", "취소"])) \
    .filter(col("category") == "여객") \
    .select(arrival_df.columns)

    
# 데이터 합치기
filtered_flights = filtered_departure.union(filtered_arrival)

# 파티션 없이 저장
# s3에 저장
filtered_flights.write \
    .mode("overwrite") \
    .parquet(target_path)


# 데이터 검증 로깅
print("=== 데이터 처리 결과 ===")
print("\n출발 데이터 처리 건수:", filtered_departure.count())
print("도착 데이터 처리 건수:", filtered_arrival.count())
print("전체 처리 건수:", filtered_flights.count())

job.commit()