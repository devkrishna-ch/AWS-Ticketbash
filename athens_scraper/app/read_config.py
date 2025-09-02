import json
import boto3

bucket_name = 'ticketbash-config'
file_key = 'config.json'
region = 'us-east-1'
s3_client = boto3.client("s3", region_name=region)


def read_config():
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    content = response["Body"].read().decode("utf-8")
    config = json.loads(content)
    return config