import boto3
import os

glue_client = boto3.client('glue')
s3_client = boto3.client('s3')

GLUE_JOB_NAME = os.environ.get('GLUE_JOB_NAME', 'glue-sukima-transform')

def lambda_handler(event, context):
    # S3バケット名とオブジェクトキーを取得
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']

    print(f"S3イベントを検知しました。バケット: {bucket_name}, ファイル: {file_key}")

    # Glueジョブを実行
    try:
        response = glue_client.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                '--S3_INPUT_PATH': f"s3://{bucket_name}/{file_key}"
            }
        )
        print(f"Glueジョブ '{GLUE_JOB_NAME}' を起動しました。")
        print(response)

    except Exception as e:
        print(f"Glueジョブの起動に失敗しました: {e}")
        raise e