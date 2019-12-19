from ftplib import FTP
from datetime import datetime
from dateutil.parser import parse
import re
import io
import os
import boto3

DATA_BUCKET = os.getenv('DATA_BUCKET')
TIME_TABLE = os.getenv('TIME_TABLE')
TODAY = datetime.today().strftime('%Y%m%d')
TODAY_F = datetime.today().strftime('%Y-%m-%d')
BASE_DIR = "OFS_Data/"+TODAY+"/HDF5/S111_1.0.0"
FILE_RE = re.compile(
    r".*((\d\d:\d\d)(AM|PM))[\ 0-9A-Z_]*Z_(.*)_TYP2(_PACIFIC|_ATLANTIC)?\.h5"
)

dynamodb = boto3.client('dynamodb')
s3 = boto3.client("s3")


def get_lastest():
    ftp = FTP('ocsftp.ncd.noaa.gov')
    ftp.login()
    ftp.cwd(BASE_DIR)
    regions = ftp.nlst()
    for region in regions:
        print("-", region)
        checked_files = []

        def check_file(x):
            res = FILE_RE.match(x)
            if res is not None:
                checked_files.append(x)
        
        ftp.cwd(region)
        ftp.retrlines('LIST', callback=check_file)
        ftp.cwd("..")

        for x in checked_files:
            res = FILE_RE.match(x)
            data_short = res.group(4)
            data_time = res.group(1)
            response = dynamodb.get_item(
                TableName=TIME_TABLE,
                Key={
                    "dataset": {
                        "S": data_short
                    }
                }
            )
            data_time = parse(TODAY_F + " " + data_time)
            refresh = ("Item" not in response or
                       float(response["Item"]["last_updated"]["S"]) < data_time.timestamp())
            if refresh:
                data = io.BytesIO()
                ftp.retrbinary(
                    'RETR %s/%s' % (region, x.split(" ")[-1]),
                    data.write)

                s3.put_object(
                    Bucket=DATA_BUCKET,
                    Key=data_short,
                    Body=data.getvalue())

                dynamodb.put_item(
                    TableName=TIME_TABLE,
                    Item={
                        'dataset': {
                            "S": data_short
                        },
                        'last_updated': {
                            "S": str(data_time.timestamp())
                        }
                    })


def lambda_handler(event, context):

    get_lastest()
    return {"statusCode": 200, "body": "Complete"}

