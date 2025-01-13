# LifeIsTravel

## aws-etc

### Glue

<img width="719" alt="스크린샷 2025-01-13 오후 4 54 51" src="https://github.com/user-attachments/assets/f9194818-fb95-459d-8f3d-b7c3a7423856" />

> 빨간색 네모 박스 부분입니다.

AWS Glue를 사용하여 데이터를 변환하고 처리하는 작업을 수행
<br>
S3 버킷 `team5-s3/raw_data`에 저장된 원시 데이터를 Glue를 통해 가공
<br>
 `team5-s3/transform_data`에 효율적인 데이터 저장 및 조회를 위해 Parquet 형식으로 저장

#### 주요 스크립트
- **team5-glue-eda.py**: EDA를 수행하여 flight_operations 데이터와 weather 데이터의 특성을 분석하고, 초기 데이터 품질을 평가
- **team5-glue-flight.py**: 일본 지역의 항공권 데이터를 일별로 처리하고 변환
- **team5-glue-flight_operations_japan_daily.py**: 일본 지역의 항공 운항 데이터를 일별로 처리하고 변환
- **team5-glue-future-weather.py**: 미래 날씨 데이터를 처리하여 예측 정보를 가공
- **team5-glue-past-weather.py**: 과거 날씨 데이터를 처리하여, 예측 정보를 가공
