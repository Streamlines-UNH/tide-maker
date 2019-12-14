import json
import boto3

# Boto S3 Client used to access buckets
s3_client = boto3.client('s3')


def lambda_handler(event, context):

    # Hardcoded to temporarily grab this file to render inital web-app map
    response = s3_client.get_object(
        Bucket='geojson-cbsc',
        Key='S111US_20191023T17Z_NYOFS_TYP2.geojson'
    )
    
    # Convert Data from byte-arrays to JSON.
    # This may not be the most efficient way of doing this...
    respData = json.loads(response['Body'].read())
    
    
    # DO NOT PRINT IT, wayyyyy too large. Log file $$$, RTT $$$
    # print(respData)
    
    return {
        'statusCode': 200,
        'body': json.dumps(respData)
    }

