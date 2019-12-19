import boto3
import os


DATA_SRC= os.getenv('DATA_SRC')
s3 = boto3.client("s3")


def lambda_handler(event, context):
    obj = s3.get_object(Bucket=DATA_SRC, Key="CBOFS/Group_001.geojson")
    body = obj["Body"].read()
    return {
        'statusCode': 200,
        'body': body
    }
