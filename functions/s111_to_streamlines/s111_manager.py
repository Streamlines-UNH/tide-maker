import subprocess
import json
import os
import boto3
import time

DATA_DEST = os.getenv('DATA_DEST')
s3 = boto3.client("s3")


def run_s111(name, group):
    env = os.environ.copy()
    env["LD_LIBRARY_PATH"] = "/opt/lib/"
    p = subprocess.Popen(["/opt/s111_to_streamlines", "/tmp/" + name, group],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         env=env)
    output = p.communicate()
    return json.loads(output[0])


def lambda_handler(event, context):

    data_path = event["Records"][0]["Sns"]["Message"].split("/")
    print(data_path)
    bucket = data_path[0]
    infile = data_path[1]
    group = data_path[2]

    print("Processing:", bucket, infile, group)
    obj = s3.get_object(Bucket=bucket, Key=infile)
    body = obj["Body"].read()
    with open("/tmp/%s" % infile, "wb") as fp:
        fp.write(body)

    outfile = infile + "/" + group + ".geojson"

    streamlines = run_s111(infile, group)
    output = json.dumps(streamlines, indent=4)



    s3.put_object(Bucket=DATA_DEST,
                  Key=outfile,
                  Body=output.encode("utf-8"))

    return {
        'statusCode': 200,
        'body': "Done"
    }
