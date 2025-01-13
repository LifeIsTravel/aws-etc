import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import mean, col, count, lit, to_timestamp, max as spark_max
from datetime import datetime, timedelta

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
bucket = "team5-s3"

# Spark context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

execution_date = datetime.now()
raw_start_date = datetime(2024, 12, 24)

# Paths
raw_path_base = f"s3://{bucket}/raw_data/weather/"
target_path = f"s3://{bucket}/transform_data/weather/future/tmp/"

# 공항 코드 목록
airports = ["ICN", "NRT", "FUK", "KIX", "CTS"]

# Load and process files from raw_path
dataframes = []
current_date = raw_start_date

while current_date <= execution_date:
    # Generate raw_path dynamically based on the date
    raw_year = current_date.strftime("%Y")
    raw_month = current_date.strftime("%m")
    raw_day = current_date.strftime("%d")
    raw_path = f"{raw_path_base}{raw_year}/{raw_month}/{raw_day}/"
    
    for airport in airports:
        try:
            # Load all relevant files for the airport
            files = spark.read.csv(f"{raw_path}{airport}_future_*.csv", header=True)
            # Add 'airport_code' column
            files = files.withColumn("airport_code", lit(airport))
            dataframes.append(files)
        except Exception as e:
            print(f"No files found for {airport} in {raw_path} or error reading files: {e}")
    
    # Move to the next day
    current_date += timedelta(days=1)

# Union all dataframes
if dataframes:
    combined_df = dataframes[0]
    for df in dataframes[1:]:
        combined_df = combined_df.union(df)
else:
    print("No new data to process.")
    combined_df = None

# Handle NaN values and remove empty rows
if combined_df:
    def calculate_mode(df, column):
        mode_row = df.groupBy(column).count().orderBy(col("count").desc()).first()
        return mode_row[column] if mode_row else None
    
    # Define NaN handling logic
    def handle_missing_values(df):
        # 모든 열이 NULL인 행 제거
        df = df.dropna(how="all")
        
        # 평균값으로 대체
        for col_name in ["temperature_2m", "relative_humidity_2m", "dew_point_2m"]:
            mean_value = df.select(mean(col(col_name))).first()[0]
            if mean_value:
                df = df.fillna({col_name: mean_value})
        
        # 0으로 대체
        for col_name in ["precipitation", "rain", "snowfall", "snow_depth", "cloud_cover", "wind_speed_10m", "wind_speed_80m", "wind_speed_120m", "wind_speed_180m", "wind_direction_10m", "wind_direction_80m", "wind_direction_120m", "wind_direction_180m"]:
            df = df.fillna({col_name: 0})
        
        # 최빈값 또는 특정 값으로 대체 (예: visibility)
        visibility_mode = calculate_mode(df, "visibility")
        if visibility_mode:
            df = df.fillna({"visibility": visibility_mode})
        
        return df

    cleaned_df = handle_missing_values(combined_df)

    # Check if the cleaned DataFrame is empty before writing
    if not cleaned_df.rdd.isEmpty():
        # Write to S3 as a single Parquet file
        cleaned_df.write.mode("overwrite").parquet(target_path)
        print(f"Data written to {target_path}")
    else:
        print("No data written as the DataFrame is empty after cleaning.")
else:
    print("No data written as no new records were found.")

job.commit()