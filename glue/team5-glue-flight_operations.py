import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
import pandas as pd

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
args['source_bucket'] = 'team5-s3'

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'])

def read_historical_data():
    """S3의 과거 데이터 Excel 파일을 Spark DataFrame으로 변환"""
    dataframes = {}
    
    for data_type in ["출발", "도착"]:
        input_path = f"s3://team5-s3/raw_data/flight_operations/parquet/flight_operations_{data_type}_20240101_to_20241222.parquet"
        try:
            # 파일 존재 여부 및 읽기 가능 여부 확인
            print(f"시도 중인 경로: {input_path}")
            spark_df = spark.read.parquet(input_path)
            
            
            '''spark_df = spark.read.format("com.crealytics.spark.excel") \
                .option("header", "true") \
                .option("treatEmptyValuesAsNulls", "true") \
                .load(input_path)
            '''
            if data_type == "출발":
                spark_df = spark_df.withColumnRenamed("출발시간", "실제시간")
            else:
                spark_df = spark_df.withColumnRenamed("도착시간", "실제시간")
                
            # 날짜 컬럼을 date 타입으로 변환
            spark_df = spark_df.withColumn("date", to_date(col("날짜"), "yyyyMMdd")) #날짜타입
            spark_df = spark_df.withColumn("date_key", regexp_replace(col("날짜"), "-", "").cast("integer")) #정수 타입
                
            dataframes[data_type] = spark_df
            print(f"{data_type} 과거 데이터 로드 완료: {input_path}")
            
            
        except Exception as e:
            print(f"{data_type} 과거 데이터 로드 실패: {str(e)}")
            raise
            
    return dataframes.get("출발"), dataframes.get("도착")

def create_historical_route_summary(dep_df, arr_df):
    """과거 데이터의 항공사별 노선 요약"""
    # 출발/도착 데이터 통합
    combined_df = dep_df.union(arr_df)
    valid_df = combined_df.filter(col("구분").isin(["화물", "여객"]))
    
    # 출발 노선 (ICN -> 외국)
    dep_routes = valid_df.filter(col("출/도착구분") == "출발") \
       .groupBy("날짜", "항공사", "도착공항코드", "도착공항명", "구분") \
       .agg(
           max("date").alias("date"),
           max("date_key").alias("date_key"),
           count("*").alias("flight_count"),
           sum(when(col("현황") == "지연", 1).otherwise(0)).alias("delayed_count"),
           sum(when(col("현황") == "취소", 1).otherwise(0)).alias("cancelled_count"),
           lit("출발").alias("route_type"),
           lit("ICN").alias("출발공항코드"),
           lit("인천공항").alias("출발공항명")
       )
   
    # 도착 노선 (외국 -> ICN)    
    arr_routes = valid_df.filter(col("출/도착구분") == "도착") \
       .groupBy("날짜", "항공사", "출발공항코드", "출발공항명", "구분") \
       .agg(
           max("date").alias("date"),
           max("date_key").alias("date_key"),
           count("*").alias("flight_count"),
           sum(when(col("현황") == "지연", 1).otherwise(0)).alias("delayed_count"),
           sum(when(col("현황") == "취소", 1).otherwise(0)).alias("cancelled_count"),
           lit("도착").alias("route_type"),
           lit("ICN").alias("도착공항코드"),
           lit("인천공항").alias("도착공항명")
       )

    # 출발/도착 데이터 통합
    route_summary = dep_routes.unionAll(arr_routes)
    
    return route_summary

def create_historical_daily_summary(dep_df, arr_df):
    """과거 데이터의 일별 운항 통계 요약"""
    # 출발/도착 데이터 통합
    combined_df = dep_df.union(arr_df)
    # 기타 제외하고 화물/여객만 필터링
    valid_df = combined_df.filter(col("구분").isin(["화물", "여객"]))
    
    # 일별 요약 통계
    daily_summary = valid_df.groupBy("날짜", "date", "date_key") \
        .agg(
            # 전체 운항 수
            count("*").alias("total_flights"),
            
            # 화물/여객 운항 수
            sum(when(col("구분") == "화물", 1).otherwise(0)).alias("cargo_flights"),
            sum(when(col("구분") == "여객", 1).otherwise(0)).alias("passenger_flights"),
            
            # 지연 운항 수
            sum(when(col("현황") == "지연", 1).otherwise(0)).alias("delayed_flights"),
            
            # 정시 운항률
            (1 - sum(when(col("현황").isin(["지연", "취소"]), 1).otherwise(0)) / count("*")).alias("on_time_performance"),
            
            # 고유 항공사 수
            countDistinct("항공사").alias("unique_airlines"),
            
            # 고유 도착지 수 (출발은 도착공항, 도착은 출발공항 기준)
            countDistinct(
                when(col("출/도착구분") == "출발", col("도착공항코드"))
                .otherwise(col("출발공항코드"))
            ).alias("unique_destinations"),
            
            # 갱신 시각
            current_timestamp().alias("update_timestamp")
        )
    
    return daily_summary

try:
    # 과거 데이터 로드
    dep_df, arr_df = read_historical_data()
    
    # 요약 데이터 생성
    route_summary = create_historical_route_summary(dep_df, arr_df)
    # Parquet로 저장
    route_summary_path = "s3://team5-s3/transform_data/flight_operations/route_summary"
    
    route_summary.write \
        .mode("overwrite") \
        .parquet(route_summary_path)
    
    # 일별 요약 통계 생성
    daily_summary = create_historical_daily_summary(dep_df, arr_df)
    # Parquet로 저장
    daily_summary_path = "s3://team5-s3/transform_data/flight_operations/daily_summary"
    daily_summary.write \
        .mode("overwrite") \
        .parquet(daily_summary_path)
    
        
    print("과거 데이터 처리 완료")
    
except Exception as e:
    print(f"데이터 처리 실패: {str(e)}")
    raise

job.commit()