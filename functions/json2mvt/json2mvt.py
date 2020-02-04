import json
import boto3
import os
import subprocess

s3_client = boto3.client("s3")

def gen_mbtiles( infile ):
    env = os.environ.copy()

    env["LD_LIBRARY_PATH"] = "/opt/lib"
    process = subprocess.Popen(["/opt/tippecanoe",
                                "-o", "tmp.mbtiles", "/tmp/", "--nofilesizelimit", "--maximum-zoom=13" ],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               env=env
                               )

def lambda_handler(event, context):

    """
    S3 File I/O Here
    """
    infile = event["Records"][0]["s3"]["key"]
    s3_obj = s3_client.get_object(
        Bucket=event["Records"][0]["s3"]["bucket"]["name"],
        key=infile
    )
    geoJson = s3_obj["Body"].read()
    localCache = open("/tmp/" + infile.replace("/", ""))
    localCache.write(geoJson)

    data_location = infile.split("/")[0]
    print(data_location)

    '''
    Generate MBTile
    '''
    mbtile = gen_mbtiles(infile)

    print("MBTILE Generated")

    """
    Slice MBTile here
    """

    return {
            'statusCode': 200,
            'body': json.dumps('Processed GeoJSON to MVT and pushed to Lambda')
           }
