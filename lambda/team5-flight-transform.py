import json
import boto3
import pandas as pd
from io import StringIO
from io import BytesIO
# import pyarrow.parquet as pq


# S3 클라이언트 생성
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Airflow에서 전달된 폴더 경로
    folder_path = event.get('folder_path')
    
    # S3 버킷과 파일 이름 설정 (예시)
    bucket_name = 'team5-s3'
    
    # 예시: folder_path가 S3 폴더 내의 경로라고 가정
    s3_prefix = folder_path
    
    # S3에서 파일 목록 가져오기
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)
    
    # 여러 개의 DataFrame을 저장할 리스트
    all_dataframes = []
    
    # 파일이 존재하면 각 파일을 처리
    for obj in response.get('Contents', []):
        file_key = obj['Key']

        # 파일명에 'empty'가 포함되어 있으면 건너뛰기
        if 'empty' in file_key:
            print(f"Skipping file with 'empty' in name: {file_key}")
            continue

        
        # S3에서 JSON 파일 읽기
        file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = file_obj['Body'].read().decode('utf-8')
        
        # JSON을 Pandas DataFrame으로 변환
        tmp_df = pd.read_json(StringIO(file_content))
        
        # DataFrame을 리스트에 추가
        all_dataframes.append(tmp_df)
        
        # 예시: 각 파일에서 처리한 DataFrame의 count 출력
        print(f"Processed file: {file_key}")
        print(f"Row count of {file_key}: {tmp_df.count()}")
    
    # 모든 DataFrame을 하나로 합치기 (중복 제거 없이)
    if all_dataframes:
        df = pd.concat(all_dataframes, ignore_index=True)
        # 최종 DataFrame의 count 출력
        print("Final Combined DataFrame (Row count):")
        print(df.count())  # 최종 DataFrame의 행 수 출력
    
    # =======================================================================================

    df = df.drop(
    columns=[
        "_segments",
        "eco",
        "has_linked_pricing_options",
        "legs",
        "pricing_option_variants",
        "pricing_options_count",
        "score"
    ]
    )
    # =======================================================================================

    # id 컬럼을 '-'로 split하여 리스트로 변환
    df['id_parts'] = df['id'].str.split('-')

    # 뒤에서부터 값을 가져오고, carriers 첫 번째 값에 '-' 추가
    df['departure_airport'] = df['id_parts'].apply(lambda x: x[0])
    df['departure_time'] = df['id_parts'].apply(lambda x: x[1])

    # _carriers_raw: '--'과 숫자 사이의 값을 추출
    df['_carriers_raw'] = df['id'].str.extract(r'--(.*?)-\d-', expand=False)

    # carriers 리스트 생성 + 첫 번째 값에 '-' 추가
    df['carriers'] = df['_carriers_raw'].str.split(',').apply(
        lambda x: [f'-{i}' if idx == 0 else i for idx, i in enumerate(x)]
    )

    # stop_count: 뒤에서 세 번째 값
    df['stop_count'] = df['id_parts'].apply(lambda x: x[-3])

    # arrival_airport: 뒤에서 두 번째 값
    df['arrival_airport'] = df['id_parts'].apply(lambda x: x[-2])

    # arrival_time: 마지막 값
    df['arrival_time'] = df['id_parts'].apply(lambda x: x[-1])

    # 중간 컬럼 제거
    df = df.drop(columns=['id_parts', '_carriers_raw'])

    # =======================================================================================

    # departure_time에서 날짜 부분만 추출하여 새로운 컬럼 'departure_date' 생성
    df['departure_date'] = df['departure_time'].str.slice(0, 6)

    # =======================================================================================

    # departure_airport에 해당하는 값을 추출
    df['departure_place_info'] = df.apply(
        lambda row: row['_places'].get(row['departure_airport'], {}) if pd.notnull(row['departure_airport']) else {},
        axis=1
    )

    # arrival_airport에 해당하는 값을 추출
    df['arrival_place_info'] = df.apply(
        lambda row: row['_places'].get(row['arrival_airport'], {}) if pd.notnull(row['arrival_airport']) else {},
        axis=1
    )

    # place_info에서 display_code와 name 추출 후 컬럼 이름 변경
    df['departure_display_code'] = df['departure_place_info'].apply(lambda x: x.get('display_code'))
    df['departure_name'] = df['departure_place_info'].apply(lambda x: x.get('name'))
    df['arrival_display_code'] = df['arrival_place_info'].apply(lambda x: x.get('display_code'))
    df['arrival_name'] = df['arrival_place_info'].apply(lambda x: x.get('name'))

    # 필요 없는 컬럼 제거
    df = df.drop(columns=['departure_airport', 'arrival_airport', 'departure_place_info', 'arrival_place_info'])

    # =======================================================================================

    # carrier_names 배열 생성: _carriers에서 carrier_id에 해당하는 'name'을 가져오기
    def get_carrier_name(carrier_id, carriers_dict):
        carrier_id = str(carrier_id)  # carrier_id를 문자열로 변환하여 딕셔너리에서 찾기
        return carriers_dict.get(carrier_id, {}).get('name', None)  # 해당 ID에 맞는 'name'을 추출

    df['carrier_names'] = df.apply(
        lambda row: [get_carrier_name(cid, row['_carriers']) for cid in row['carriers']] if isinstance(row['carriers'], list) and row['carriers'] else [],
        axis=1
    )

    # carriers 컬럼 제거
    df = df.drop(columns=['carriers'])

    # =======================================================================================

    # cheapest_price 컬럼이 이미 dictionary라면
    df['amount'] = df['cheapest_price'].apply(lambda x: x.get('amount', None) if isinstance(x, dict) else None)
    df['last_updated'] = df['cheapest_price'].apply(lambda x: x.get('last_updated', None) if isinstance(x, dict) else None)

    # =======================================================================================

    # 'pricing_options'에서 조건에 맞는 'agent_id'와 'url' 추출
    def extract_agent_id_and_url(pricing_options, target_amount, target_last_updated):
        agent_ids = []
        urls = []
        for option in pricing_options:
            try:
                # 가격과 마지막 업데이트가 일치하는지 확인
                price = option.get('price', {})
                if price.get('amount') == target_amount and price.get('last_updated') == target_last_updated:
                    for item in option.get('items', []):
                        agent_id = option.get('agent_ids', [None])[0]  # agent_ids에서 첫 번째 값만 사용
                        agent_ids.append(agent_id)  # 첫 번째 agent_id만 저장
                        urls.append('https://www.skyscanner.co.kr' + item.get('url', ''))  # url이 없을 경우 처리
            except KeyError as e:
                print(f"KeyError: {e} in option {option}")
        return agent_ids, urls

    # 'amount'와 'last_updated'를 이용하여 'pricing_options'에서 필요한 데이터 추출
    df[['agent_ids', 'url']] = df.apply(
        lambda row: pd.Series(extract_agent_id_and_url(row['pricing_options'], 
                                                    row['cheapest_price']['amount'],
                                                    row['cheapest_price']['last_updated'])),
        axis=1
    )

    # =======================================================================================

    # agent_ids의 첫 번째 값으로 agent_name 추출
    df['agent_name'] = df.apply(
        lambda row: row['_agents'].get(row['agent_ids'][0], {}).get('name', None) if row['agent_ids'] else None,
        axis=1
    )

    # =======================================================================================
    # 슬래시 기준으로 경로를 나누기
    parts = folder_path.split("/")

    # 연도, 월, 일, 시간, 분을 추출
    year = parts[2]
    month = parts[3]
    day = parts[4]
    time = parts[5].replace("-", "")  # "03-04" -> "0304"

    # 원하는 포맷으로 합치기
    formatted_str = year + month + day + time

    df['extracted_at'] = formatted_str

    # =======================================================================================

    # 'id', '_agents', '_carriers', '_places', 'cheapest_price', 'pricing_options' 열 삭제
    df = df.drop(columns=["id", "_agents", "_carriers", "_places", "cheapest_price", "pricing_options"])


    # 필요한 열만 선택
    df = df[[
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
    ]]

    # =======================================================================================
    print(df.head(3))
    print(df.count())

    # =======================================================================================
    # DataFrame을 Parquet 형식으로 변환
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')

    s3_file_key = folder_path.replace("raw_data", "transform_data") + "final_data.parquet"

    # S3에 Parquet 파일로 저장
    s3_client.put_object(Bucket=bucket_name, Key=s3_file_key, Body=parquet_buffer.getvalue())
        
    # S3에 파일 업로드 성공 메시지 출력
    print(f"File successfully uploaded to S3: {s3_file_key}")
    
    
    # # DataFrame을 JSON 형식으로 변환
    # json_buffer = StringIO()
    # df.to_json(json_buffer, orient='records', lines=True)
        
    # # S3에 JSON 파일로 저장
    # s3_file_key = folder_path.replace("raw_data", "transform_data") + "final_data.json"
    # s3_client.put_object(Bucket=bucket_name, Key=s3_file_key, Body=json_buffer.getvalue())
    
    # # S3에 파일 업로드 성공 메시지 출력
    # print(f"File successfully uploaded to S3: {s3_file_key}")

    return {
        'statusCode': 200,
        'body': json.dumps(f"Processed files from folder: {folder_path}")
    }
