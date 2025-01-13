import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import mean, col, lit, to_timestamp, max as spark_max
from datetime import datetime, timedelta

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 경로 설정
raw_base_path = "s3://team5-s3/raw_data/weather/"
base_path = "s3://team5-s3/raw_data/weather/past/"
target_path = "s3://team5-s3/transform_data/weather/past/"
airports = ["ICN", "NRT", "FUK", "KIX", "CTS"]

dataframes = []
try:
    existing_df = spark.read.parquet(target_path)
    latest_timestamp = existing_df.select(spark_max("timestamp").alias("latest")).collect()[0]["latest"]
    print(f"Latest timestamp found in existing data: {latest_timestamp}")
except Exception as e:
    for airport in airports:
        file_path = f"{base_path}{airport}_20240101_20241228.csv"
        df = spark.read.csv(file_path, header=True)
        df = df.withColumn("airport_code", lit(airport))
        dataframes.append(df)
        
    print("No existing data found. Starting fresh.")
    latest_timestamp = None

# Load and process files from raw_path

for airport in airports:
    current_date = datetime(2024, 12, 24)  # 시작 날짜
    execution_date = datetime.now()  # 현재 날짜

    while current_date <= execution_date:
        # Generate raw_path dynamically based on the date
        raw_year = current_date.strftime("%Y")
        raw_month = current_date.strftime("%m")
        raw_day = current_date.strftime("%d")
        raw_path = f"{raw_base_path}{raw_year}/{raw_month}/{raw_day}/"
        
        try:
            # Load all relevant files for the airport
            file_path = f"{raw_path}{airport}_past_*.csv"
            df = spark.read.csv(file_path, header=True)
            
            # Add 'airport_code' column
            df = df.withColumn("airport_code", lit(airport))
            
            # Filter for incremental updates if latest_timestamp exists
            if latest_timestamp:
                df = df.filter(to_timestamp(col("timestamp")) > latest_timestamp)
            
            dataframes.append(df)
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

# Handle NaN values
if combined_df:
    # Define NaN handling logic
    def handle_missing_values(df):
        # 평균값으로 대체
        for col_name in ["temperature_2m", "relative_humidity_2m", "dew_point_2m"]:
            mean_value = df.select(mean(col(col_name))).first()[0]
            if mean_value:
                df = df.fillna({col_name: mean_value})
        
        # 0으로 대체
        for col_name in [
            "precipitation", "rain", "snowfall", "snow_depth", "cloud_cover", 
            "wind_speed_10m", "wind_speed_100m", "wind_gusts_10m", 
            "wind_direction_10m", "wind_direction_100m"
        ]:
            df = df.fillna({col_name: 0})
        
        return df

    cleaned_df = handle_missing_values(combined_df)

    if not cleaned_df.rdd.isEmpty():
        cleaned_df.write.mode("append").parquet(target_path)
        print(f"Data written to {target_path}")
else:
    print("No data written as no new records were found.")

job.commit()