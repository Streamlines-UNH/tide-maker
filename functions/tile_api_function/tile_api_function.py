import os
import boto3
import base64


dynamodb = boto3.client('dynamodb')
s3_client = boto3.client("s3")
DATA_TABLE = os.getenv('DATA_TABLE')
DATA_BUCKET = os.getenv('DATA_BUCKET')


def lambda_handler(event, context):
    region = event["pathParameters"]["region"]
    t = event["pathParameters"]["t"]
    z = event["pathParameters"]["z"]
    x = event["pathParameters"]["x"]
    y = os.path.splitext(event["pathParameters"]["y"])[0]
    table_index = "{}-{}-{}-{}-{}".format(region, t, z, x, y)
    res = dynamodb.get_item(
        TableName=DATA_TABLE,
        Key={
            "tileKey": {
                "S": table_index
            }
        }
    )
    if "Item" not in res:
        return {
            'statusCode': 204,
        }
    if res["Item"]["huge"]["BOOL"]:
        s3_obj = s3_client.get_object(
            Bucket=DATA_BUCKET,
            Key=table_index
        )
        data = s3_obj["Body"].read()
    else:
        data = res["Item"]["tile"]["B"]
    return {
        "isBase64Encoded": True,
        "statusCode": 200,
        "headers": {
            "content-type": "application/x-protobuf",
            "content-encoding": "gzip",
        },
        "body":  base64.b64encode(data).decode("utf-8"),
        "Access-Control-Allow-Origin": "*"
    }
