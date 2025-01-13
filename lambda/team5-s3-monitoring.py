import boto3
import json
import urllib.parse


s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

# Glue 작업 이름
GLUE_JOB_NAME = 'team5-glue-test'

def lambda_handler(event, context):
    # 이벤트 객체에서 버킷 이름과 객체 키를 추출
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    print(f"bucket_name : {bucket_name}")
    print(f"object_key : {object_key}")

    decoded_object_key = urllib.parse.unquote(object_key)
    
    # 폴더 경로 추출 (예: flights/2024/12/20/23:00/)
    folder_path = "/".join(decoded_object_key.split("/")[:-1]) + "/"
    print(f"folder_path : {folder_path}")
    
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=folder_path
        )
        
        # 파일 개수 확인
        if 'Contents' in response:
            file_count = sum(1 for obj in response['Contents'] if obj['Key'].endswith('.parquet'))
            
            # 파일 개수가 80개일 경우 Glue 작업 실행
            if file_count == 80:
                print(f"폴더에 파일이 {file_count}개 있습니다. Glue 작업을 실행합니다.")
                
                # Glue 작업에 전달할 인자
                glue_arguments = {
                    '--folder_path': folder_path
                }
                
                # Glue 작업 실행
                glue_response = glue_client.start_job_run(
                    JobName=GLUE_JOB_NAME,
                    Arguments=glue_arguments  # Arguments로 경로 정보를 전달
                )
                print(f"Glue 작업이 성공적으로 시작되었습니다. JobRunId: {glue_response['JobRunId']}")
                return {
                    'statusCode': 200,
                    'body': json.dumps('Glue 작업이 성공적으로 시작되었습니다.')
                }
            else:
                print(f"파일 개수: {file_count} (Glue 작업을 실행하지 않습니다.)")
                return {
                    'statusCode': 400,
                    'body': json.dumps(f"파일 개수: {file_count} (80개가 아닙니다)")
                }
        else:
            print("해당 폴더에 파일이 없습니다.")
            return {
                'statusCode': 404,
                'body': json.dumps('해당 폴더에 파일이 없습니다.')
            }
    
    except Exception as e:
        print(f"오류 발생: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"서버 오류 발생: {str(e)}")
        }
