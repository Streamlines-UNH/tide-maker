import json
import boto3
import os
import re
import subprocess

"""
Python TippeCanoe binary wrapper.
@author github:@StreamlinesUNH
Modified into a cloud-native SQL Egress via Lambda to AWS DynamoDB
"""

from mbutil import mbtiles_to_disk, to_geo

s3_client = boto3.client("s3")
dynamodb = boto3.client("dynamodb")
TIME_TABLE = os.getenv("TIME_TABLE")


def gen_mbtiles(infile):
    env = os.environ.copy()

    env["LD_LIBRARY_PATH"] = "/opt/lib"
    process = subprocess.Popen(
        [
            "/opt/tippecanoe",
            "-o",
            "/tmp/" + infile + ".mbtiles",
            "/tmp/" + infile + ".geojson",
            "--maximum-zoom=14",
            "-pk",
            "-pc",
            "-pD",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )

    return process


def lambda_handler(event, context):
    """
    S3 File I/O Here
    REAL I/O
    """
    infile = event["Records"][0]["s3"]["object"]["key"]
    s3_obj = s3_client.get_object(
        Bucket=event["Records"][0]["s3"]["bucket"]["name"], Key=infile
    )

    data_location = infile.split("/")[0]
    infile = (infile.replace("/", "")).split(".")[0]

    print("Infile is " + data_location + "-" + str(int(re.findall(r"\d+", infile)[0])))

    geoJson = s3_obj["Body"].read()
    localCache = open("/tmp/" + infile + ".geojson", "wb")
    localCache.write(geoJson)
    localCache.close()

    """
    Generate MBTile & store it at: /tmp/tmp.mbtiles
    """
    process = gen_mbtiles(infile)
    process.wait()

    print("MBTILE Generated")

    response = dynamodb.get_item(
        TableName=TIME_TABLE, Key={"dataset": {"S": data_location}}
    )
    if "Item" not in response:
        return {"statusCode": 404, "body": "Failed time table lookup"}

    """
    Slice MBTile here
    """
    mbtiles_to_disk(
        "/tmp/" + infile + ".mbtiles",
        data_location + "-" + str(int(re.findall(r"\d+", infile)[0])),
        response["Item"]["last_updated"]["S"],
    )
    # In testing Lambda disk was full when this function was slammed
    # /tmp acks as a temporary cache between invocations so
    # we should cleanup a bit
    try: 
        os.remove("/tmp/" + infile + ".mbtiles") 
        os.remove("/tmp/" + infile + ".geojson") 
    except OSError as error:
        # we dont realy need to care if this fails 
        pass


    return {
        "statusCode": 200,
        "body": json.dumps("Processed GeoJSON to MVT and pushed to Lambda"),
    }
