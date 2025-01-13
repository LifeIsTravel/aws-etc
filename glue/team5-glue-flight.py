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


# json 스키마 정의
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType, MapType, ArrayType, FloatType


agents_schema = StructType([
    StructField("id", StringType(), True),
    StructField("feedback_count", IntegerType(), True),
    StructField("is_carrier", BooleanType(), True),
    StructField("live_update_allowed", BooleanType(), True),
    StructField("name", StringType(), True),
    StructField("optimised_for_mobile", BooleanType(), True),
    StructField("rating", DoubleType(), True),
    StructField("rating_breakdown", StructType([
        StructField("clear_extra_fees", IntegerType(), True),
        StructField("customer_service", IntegerType(), True),
        StructField("ease_of_booking", IntegerType(), True),
        StructField("other", IntegerType(), True),
        StructField("reliable_prices", IntegerType(), True)
    ]), True),
    StructField("rating_status", StringType(), True),
    StructField("update_status", StringType(), True)
])

carriers_schema = MapType(
    StringType(),
    StructType([
        StructField("alt_id", StringType(), True),
        StructField("code", StringType(), True),
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True)
    ]),
    True
)

place_schema = StructType([
    StructField("alt_id", StringType(), True),
    StructField("display_code", StringType(), True),
    StructField("entity_id", IntegerType(), True),
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("parent_entity_id", IntegerType(), True),
    StructField("parent_id", IntegerType(), True),
    StructField("type", StringType(), True)
])

# _segments 내부의 각 segment에 대한 스키마
segment_schema = StructType([
    StructField("arrival", StringType(), True),
    StructField("departure", StringType(), True),
    StructField("destination_place_id", IntegerType(), True),
    StructField("duration", IntegerType(), True),
    StructField("id", StringType(), True),
    StructField("marketing_carrier_id", IntegerType(), True),
    StructField("marketing_flight_number", StringType(), True),
    StructField("mode", StringType(), True),
    StructField("operating_carrier_id", IntegerType(), True),
    StructField("origin_place_id", IntegerType(), True)
])

# cheapest_price 스키마
cheapest_price_schema = StructType([
    StructField("amount", IntegerType(), True),
    StructField("last_updated", StringType(), True),
    StructField("quote_age", IntegerType(), True),
    StructField("update_status", StringType(), True)
])

# eco 스키마
eco_schema = StructType([
    StructField("eco_contender_delta", FloatType(), True),
    StructField("emissions_delta_in_kg", FloatType(), True),
    StructField("emissions_delta_percentage", FloatType(), True),
    StructField("emissions_in_kg", FloatType(), True),
    StructField("emissions_level", StringType(), True),
    StructField("is_eco_contender", BooleanType(), True),
    StructField("typical_emissions_in_kg", FloatType(), True)
])

# legs 내부의 각 leg에 대한 스키마
leg_schema = StructType([
    StructField("arrival", StringType(), True),
    StructField("departure", StringType(), True),
    StructField("destination_place_id", IntegerType(), True),
    StructField("duration", IntegerType(), True),
    StructField("id", StringType(), True),
    StructField("marketing_carrier_ids", ArrayType(IntegerType()), True),
    StructField("operating_carrier_ids", ArrayType(IntegerType()), True),
    StructField("origin_place_id", IntegerType(), True),
    StructField("segment_ids", ArrayType(StringType()), True),
    StructField("self_transfer", BooleanType(), True),
    StructField("stop_count", IntegerType(), True),
    StructField("stop_ids", ArrayType(StringType()), True)
])

# pricing_option_variants 스키마
# JSON 데이터가 빈 배열([])로 제공되었으므로 기본적으로 빈 ArrayType(StringType())으로 정의
pricing_option_variants_schema = ArrayType(StringType())

items_schema = StructType([
    StructField("agent_id", StringType(), True),
    StructField("booking_metadata", StructType([
        StructField("metadata_set", StringType(), True),
        StructField("signature", StringType(), True)
    ]), True),
    StructField("booking_proposition", StringType(), True),
    StructField("discount_category", StringType(), True),
    StructField("fares", ArrayType(StructType([
        StructField("segment_id", StringType(), True)
    ])), True),
    StructField("flight_attributes", ArrayType(StringType()), True),
    StructField("max_redirect_age", IntegerType(), True),
    StructField("opaque_id", StringType(), True),
    StructField("price", StructType([
        StructField("amount", IntegerType(), True),
        StructField("last_updated", StringType(), True),
        StructField("quote_age", IntegerType(), True),
        StructField("update_status", StringType(), True)
    ]), True),
    StructField("segment_ids", ArrayType(StringType()), True),
    StructField("ticket_attributes", ArrayType(StringType()), True),
    StructField("transfer_protection", StringType(), True),
    StructField("url", StringType(), True)
])

# pricing_options 스키마
pricing_options_schema = StructType([
    StructField("agent_ids", ArrayType(StringType()), True),
    StructField("id", StringType(), True),
    StructField("items", ArrayType(items_schema), True),
    StructField("price", StructType([
        StructField("amount", IntegerType(), True),
        StructField("last_updated", StringType(), True),
        StructField("quote_age", IntegerType(), True),
        StructField("update_status", StringType(), True)
    ]), True),
    StructField("pricing_option_fare", StructType([
        StructField("added_attribute_labels", ArrayType(StringType()), True),
        StructField("attribute_labels", ArrayType(StringType()), True),
        StructField("brand_names", ArrayType(StringType()), True),
        StructField("leg_details", MapType(StringType(), StringType()), True)
    ]), True),
    StructField("pricing_option_fare_type", StringType(), True),
    StructField("score", FloatType(), True),
    StructField("transfer_type", StringType(), True),
    StructField("unpriced_type", StringType(), True)
])

# 전체 JSON의 스키마
main_schema = StructType([
    StructField("_agents", MapType(StringType(), agents_schema), True),             # _agents 스키마
    StructField("_carriers", carriers_schema, True),                                # _carriers 스키마
    StructField("_places", MapType(StringType(), place_schema), True),              # _places 스키마
    StructField("_segments", MapType(StringType(), segment_schema), True),          # _segments 스키마
    StructField("cheapest_price", cheapest_price_schema, True),                     # cheapest_price 스키마
    StructField("eco", eco_schema, True),                                           # eco 스키마
    StructField("has_linked_pricing_options", BooleanType(), True),                 # has_linked_pricing_options 필드
    StructField("id", StringType(), True),                                          # id 필드
    StructField("legs", ArrayType(leg_schema), True),                               # legs 스키마 추가
    StructField("pricing_option_variants", pricing_option_variants_schema, True),   # pricing_option_variants 스키마 추가
    StructField("pricing_options", ArrayType(pricing_options_schema), True),        # pricing_options 스키마 추가
    StructField("pricing_options_count", IntegerType(), True),                      # pricing_options_count 필드 추가
    StructField("score", FloatType(), True)                                         # score 필드 추가
])


# 파일이 존재할 경우에만 읽기
if json_files:
    df = spark.read.schema(main_schema).option("multiline","true").option("mode", "PERMISSIVE").json(json_files)
    df.show(3)
    print("파일 존재")
else:
    print("유효한 json 파일 없음")
    
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

df = df.withColumn("extracted_at", lit(formatted_str))

# ==================================================================================================================================================
# 일단 지금 필요없는 column 삭제

df = df.drop(
    "_segments",
    "eco",
    "has_linked_pricing_options",
    "legs",
    "pricing_option_variants",
    "pricing_options_count",
    "score",
)

# ==================================================================================================================================================
# ID 분해하기

#기존 id
#출발공항고유번호-출발시간-항공사-경유횟수-도착공항고유번호-도착시간
#분해해서 df에 붙이기

from pyspark.sql.functions import col, split, regexp_extract, size, expr


# Split id 컬럼을 리스트로 분리
df_split = df.withColumn("id_parts", split(col("id"), "-"))

# 뒤에서부터 값 가져오기 + carriers 첫 번째 값에 '-' 추가
df = (
    df_split.withColumn("departure_airport", col("id_parts")[0])
    .withColumn("departure_time", col("id_parts")[1])
    .withColumn("_carriers_raw", regexp_extract(col("id"), r"--(.*?)-\d-", 1))
    .withColumn(
        "carriers",
        expr(
            """
        transform(split(_carriers_raw, ","), 
        (x, i) -> CASE WHEN i = 0 THEN concat('-', x) ELSE x END
        )
    """
        ),
    )  # carriers 리스트 생성 + 첫 번째 값에 '-' 추가
    .withColumn(
        "stop_count", col("id_parts")[size(col("id_parts")) - 3]
    )  # 뒤에서 세 번째
    .withColumn(
        "arrival_airport", col("id_parts")[size(col("id_parts")) - 2]
    )  # 뒤에서 두 번째
    .withColumn("arrival_time", col("id_parts")[size(col("id_parts")) - 1])  # 마지막
    .drop("id_parts", "_carriers_raw")  # 중간 컬럼 제거
)

# ==================================================================================================================================================
# 출발일 추출하기

#출발시간에서 출발날짜 추출

from pyspark.sql.functions import col, substring

# departure_time에서 날짜 부분만 추출하여 새로운 컬럼 'departure_date' 생성
df = df.withColumn("departure_date", substring(col("departure_time"), 1, 6))

# ==================================================================================================================================================
# 출발 공항 코드, 이름 / 도착 공항 코드, 이름 추출하기

#id에서는 공항의 번호가 나와있다
#공항번호를 가지고 '_places' 컬럼과 비교하며 공항코드와 공항이름 가져왔다

from pyspark.sql.functions import col

# departure_airport에 해당하는 값을 추출
df_with_departure_places = df.withColumn(
    "departure_place_info",
    col("_places").getItem(
        col("departure_airport")
    ),  # departure_airport 값에 해당하는 _places 정보 가져오기
)

# arrival_airport에 해당하는 값을 추출
df_with_final_places = df_with_departure_places.withColumn(
    "arrival_place_info",
    col("_places").getItem(
        col("arrival_airport")
    ),  # arrival_airport 값에 해당하는 _places 정보 가져오기
)

# place_info에서 display_code와 name 추출 후 컬럼 이름 변경
df = (
    df_with_final_places.withColumn(
        "departure_display_code", col("departure_place_info")["display_code"]
    )
    .withColumn("departure_name", col("departure_place_info")["name"])
    .withColumn("arrival_display_code", col("arrival_place_info")["display_code"])
    .withColumn("arrival_name", col("arrival_place_info")["name"])
)
df = df.drop(
    "departure_airport", "arrival_airport", "departure_place_info", "arrival_place_info"
)

# ==================================================================================================================================================
# 항공사 추출하기

#id에는 항공사의 번호가 나와있다
#항공사번호를 가지고 '_carriers' 컬럼과 비교하며 항공사이름을 가져왔다
#배열 형태로 저장되어 있는데, 경유일떄는 항공사가 2곳 일 수도 있기 때문이다

from pyspark.sql.functions import expr

# _carriers 컬럼에서 carrier_id를 기준으로 name을 추출하여 carrier_names 배열을 만들기
df = df.withColumn(
    "carrier_names",
    # carriers 배열을 돌면서 _carriers에서 해당 ID에 맞는 'name'을 가져오기
    expr(
        "TRANSFORM(carriers, carrier_id -> _carriers[CAST(carrier_id AS STRING)].name)"
    ),
)
df = df.drop("carriers")

# ==================================================================================================================================================
# 가격, last_updated 추출하기

#'cheapest_price' 컬럼에서 가장 싼 값, 언제 업데이트 되었는지에 대한 데이터를 가져왔다

from pyspark.sql.functions import col

# cheapest_price에서 amount, last_updated, quote_age 추출
df = (
    df.withColumn("amount", col("cheapest_price.amount"))
    .withColumn("last_updated", col("cheapest_price.last_updated"))
)

# ==================================================================================================================================================
# url 추출하기

#위에서 가장 싼 값(amount), 업데이트 시간(last_updated)을 알아냈다
#이걸 가지고 'pricing_options' 컬럼을 돌면서 같은 amount, 같은 last_updated을 발견하면 해당 데이터에서 'agent_id'와 'url'을 가져왔다
#'agent_id'는 이후 바우처 추출에서 쓰인다

from pyspark.sql.functions import col, concat, lit, explode

# pricing_options 컬럼을 explode
df_with_json = df.withColumn(
    "pricing_options", explode(col("pricing_options"))
)

# pricing_options에서 amount와 last_updated 값 추출
df_with_json = df_with_json.withColumn(
    "pricing_amount", col("pricing_options.price.amount").cast("double")
)
df_with_json = df_with_json.withColumn(
    "pricing_last_updated", col("pricing_options.price.last_updated")
)

# df의 amount와 last_updated를 가격 옵션에서 일치하는 값으로 필터링
df_filtered = df_with_json.filter(
    (col("amount").cast("double") == col("pricing_amount"))
    & (col("last_updated") == col("pricing_last_updated"))
)

# 일치하는 pricing_options에서 agent_ids와 url 추출
df_result = df_filtered.select(
    "id",  # df의 id 컬럼을 가져옵니다 (join을 위한 키)
    col("pricing_options.agent_ids")[0].alias("agent_id"),
    concat(
        lit("https://www.skyscanner.co.kr"), col("pricing_options.items")[0].url
    ).alias(
        "url"
    ),  # URL 앞부분 추가
)

# df와 df_result를 id 기준으로 join하여 df에 agent_ids와 url 컬럼 추가
df = df.join(df_result, on="id", how="left")

# ==================================================================================================================================================
# 바우처 추출하기

#위에서 알아낸 '_agents'를 알아냈다
#이걸 가지고 '_agents'를 돌면서 같은 '_agents'을 발견하면 해당 데이터에서 'agent_name'을 가져왔다

# _agents에서 agent_id와 일치하는 키가 있는지 확인하고 해당 키의 name 값을 가져오기
df_with_agents = df.withColumn(
    "agent_name",
    expr(
        """
        CASE 
            WHEN array_contains(map_keys(_agents), agent_id) THEN _agents[agent_id]['name']
            ELSE NULL
        END
    """
    ),
)

# 기존 df와 결합하여 agent_name 추가 (기존 df를 기반으로 하므로 df로 마무리)
df = df.join(df_with_agents.select("id", "agent_name"), on="id", how="left")
df = df.drop("agent_id")

# ==================================================================================================================================================
# 마무리

#필요없는 컬럼 지우기, 순서 맞추기

df = df.drop(
    "id", "_agents", "_carriers", "_places", "cheapest_price", "pricing_options"
)
df = df.select(
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
df.write.parquet(save_path, mode='append')

# RDS를 위해 CSV 파일도 일단 저장
from pyspark.sql.functions import array_join

df = df.withColumn("carrier_names", array_join("carrier_names", ","))
df.write.option("header", "true").csv(save_path, mode='append')

# ==================================================================================================================================================
# ==================================================================================================================================================
# ==================================================================================================================================================

print()
print("transform 완료된 df")
df.show(3)

# SparkContext 정리
sc.stop()

print("ETL 작업이 성공적으로 완료되었습니다.")
