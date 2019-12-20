import boto3
import io
import os
import h5py
import time

DATA_DEST = os.getenv('UPDATE_TABLE')
dynamodb = boto3.client('dynamodb')
s3 = boto3.client("s3")


def split_groups(dataset, infile, bucket):
    surf_cur_group = dataset["SurfaceCurrent"]
    for key in surf_cur_group:
        if key == "axisNames":
            continue
        data = surf_cur_group[key]
        for name in data:
            if name == "uncertainty":
                continue
            data_path = "%s/%s/%s" % (bucket, infile, name)
            dynamodb.put_item(
                TableName=DATA_DEST,
                Item={
                    'key_name': {
                        "S": data_path
                    },
                    'ts': {
                        "S": str(time.time())
                    }
                })


def lambda_handler(event, context):

    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    infile = event["Records"][0]["s3"]["object"]["key"]

    obj = s3.get_object(Bucket=bucket, Key=infile)
    body = obj["Body"].read()
    data = io.BytesIO()
    data.write(body)

    dataset = h5py.File(data, "r")

    split_groups(dataset, infile, bucket)

    return {"statusCode": 200, "body": "Complete"}
