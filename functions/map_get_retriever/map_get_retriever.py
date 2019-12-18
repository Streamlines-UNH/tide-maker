import boto3
import os


TABLE = os.getenv('DATA_SRC')
client = boto3.client('dynamodb')


def lambda_handler(event, context):
    response = client.get_item(
        TableName=TABLE,
        Key={
            "source_name": {
                "S": "S111US_20191023T17Z_NYOFS_TYP2_Group_001"
            }
        }
    )
    respData = response["Item"]
    return {
        'statusCode': 200,
        'body': str(respData)
    }
