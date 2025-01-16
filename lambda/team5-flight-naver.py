import json
import random
import time
import boto3

import requests


def lambda_handler(event, context):
    s3_key = event.get('s3_key')
    date = event.get('date')
    origin = event.get('origin')
    target = event.get('target')
    bucket_name = 'team5-s3'

    url = "https://airline-api.naver.com/graphql"
    headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "ko,en-US;q=0.9,en;q=0.8,ko-KR;q=0.7",
        "content-type": "application/json",
        "origin": "https://flight.naver.com",
        "referer": f"https://flight.naver.com/flights/international/{origin}-{target}-{date}?adult=1&fareType=Y",
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
    }

    payload1 = {
        "operationName": "getInternationalList",
        "query": "query getInternationalList($trip: InternationalList_TripType!, $itinerary: [InternationalList_itinerary]!, $adult: Int = 1, $child: Int = 0, $infant: Int = 0, $fareType: InternationalList_CabinClass!, $where: InternationalList_DeviceType = pc, $isDirect: Boolean = false, $stayLength: String, $galileoKey: String, $galileoFlag: Boolean = true, $travelBizKey: String, $travelBizFlag: Boolean = true) {\n  internationalList(\n    input: {trip: $trip, itinerary: $itinerary, person: {adult: $adult, child: $child, infant: $infant}, fareType: $fareType, where: $where, isDirect: $isDirect, stayLength: $stayLength, galileoKey: $galileoKey, galileoFlag: $galileoFlag, travelBizKey: $travelBizKey, travelBizFlag: $travelBizFlag}\n  ) {\n    galileoKey\n    galileoFlag\n    travelBizKey\n    travelBizFlag\n    totalResCnt\n    resCnt\n    results {\n      airlines\n      airports\n      fareTypes\n      schedules\n      fares\n      errors\n      carbonEmissionAverage {\n        directFlightCarbonEmissionItineraryAverage\n        directFlightCarbonEmissionAverage\n      }\n    }\n  }\n}",
        "variables": {
            "adult": 1,
            "child": 0,
            "fareType": "Y",
            "galileoFlag": True,
            "galileoKey": "",
            "infant": 0,
            "isDirect": True,
            "itinerary": [
                {
                    "arrivalAirport": f"{target}",
                    "departureAirport": f"{origin}",
                    "departureDate": f"{date}",
                }
            ],
            "stayLength": "",
            "travelBizFlag": True,
            "travelBizKey": "",
            "trip": "OW",
            "where": "pc",
        },
    }

    payload2 = {
        "operationName": "getInternationalList",
        "query": "query getInternationalList($trip: InternationalList_TripType!, $itinerary: [InternationalList_itinerary]!, $adult: Int = 1, $child: Int = 0, $infant: Int = 0, $fareType: InternationalList_CabinClass!, $where: InternationalList_DeviceType = pc, $isDirect: Boolean = false, $stayLength: String, $galileoKey: String, $galileoFlag: Boolean = true, $travelBizKey: String, $travelBizFlag: Boolean = true) {\n  internationalList(\n    input: {trip: $trip, itinerary: $itinerary, person: {adult: $adult, child: $child, infant: $infant}, fareType: $fareType, where: $where, isDirect: $isDirect, stayLength: $stayLength, galileoKey: $galileoKey, galileoFlag: $galileoFlag, travelBizKey: $travelBizKey, travelBizFlag: $travelBizFlag}\n  ) {\n    galileoKey\n    galileoFlag\n    travelBizKey\n    travelBizFlag\n    totalResCnt\n    resCnt\n    results {\n      airlines\n      airports\n      fareTypes\n      schedules\n      fares\n      errors\n      carbonEmissionAverage {\n        directFlightCarbonEmissionItineraryAverage\n        directFlightCarbonEmissionAverage\n      }\n    }\n  }\n}",
        "variables": {
            "adult": 1,
            "child": 0,
            "fareType": "Y",
            "galileoFlag": False,
            "galileoKey": "",
            "infant": 0,
            "isDirect": True,
            "itinerary": [
                {
                    "arrivalAirport": f"{target}",
                    "departureAirport": f"{origin}",
                    "departureDate": f"{date}",
                }
            ],
            "stayLength": "",
            "travelBizFlag": False,
            "travelBizKey": "",
            "trip": "OW",
            "where": "pc",
        },
    }

    try:
        print(f"{origin} -> {target}의 {date} Naver Flight 1번째 요청 보내는 중...")
        response = requests.post(url, json=payload1, headers=headers)
        print(f"{origin} -> {target}의 {date} 의 1번째 status code: {response.status_code}")
        response.raise_for_status()
        response_data = response.json()

        travel_biz_key = response_data["data"]["internationalList"]["travelBizKey"]
        galileo_key = response_data["data"]["internationalList"]["galileoKey"]

        time.sleep(random.uniform(10, 13))

        payload2["variables"].update({
            "galileoFlag": bool(galileo_key),
            "galileoKey": galileo_key,
            "travelBizFlag": bool(travel_biz_key),
            "travelBizKey": travel_biz_key,
        })

        print(f"{origin} -> {target}의 {date} Naver Flight 2번째 요청 보내는 중...")
        response2 = requests.post(url, json=payload2, headers=headers)
        print(f"{origin} -> {target}의 {date} 의 2번째 status code: {response2.status_code}")
        response2.raise_for_status()
        response_data2 = response2.json()

        # if response_data2["data"]["internationalList"]["resCnt"] == 0 and retry_count > 0:
        #     return await get_flight_data_naver(origin, target, date, retry_count - 1)

        print(f"{origin} -> {target}의 {date} Naver Flight 결과 가져오기 완료.")

        s3_client = boto3.client('s3')
        json_data = json.dumps(response_data2, ensure_ascii=False, indent=4)

        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json_data
        )

        print(f"{origin} -> {target}의 {date} S3에 저장 완료.")


    except requests.exceptions.HTTPError as http_err:
        print(f"ERROR: HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"ERROR: Request error occurred: {req_err}")
    except Exception as e:
        print(f"ERROR: Naver Flight 실행 중 오류 발생: {str(e)}")
