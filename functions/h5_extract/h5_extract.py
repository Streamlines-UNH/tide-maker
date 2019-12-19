import boto3
import io
import os
import h5py

DATA_DEST = os.getenv('DATA_DEST')
dynamodb = boto3.client('dynamodb')
s3 = boto3.client("s3")


def split_groups(dataset, pre):
    surf_cur_group = dataset["SurfaceCurrent"]
    for key in surf_cur_group:
        if key == "axisNames":
            continue
        data = surf_cur_group[key]
        for name in data:
            if name == "uncertainty":
                continue
            fp = io.BytesIO()
            with h5py.File(fp, "r+") as h5f:
                gr = h5f.create_group(name)
                gr.create_dataset("values", data=data[name]["values"])
                for x in data.attrs:
                    h5f.attrs[x] = data.attrs[x]
                for x in data[name].attrs:
                    h5f[name].attrs[x] = data[name].attrs[x]
                h5f.close()

            outfile = pre + "/" + name

            s3.put_object(
                Bucket=DATA_DEST,
                Key=outfile,
                Body=fp.getvalue())


def lambda_handler(event, context):

    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    infile = event["Records"][0]["s3"]["object"]["key"]

    obj = s3.get_object(Bucket=bucket, Key=infile)
    body = obj["Body"].read()
    data = io.BytesIO()
    data.write(body)

    pre, _ = os.path.splitext(infile)

    dataset = h5py.File(data, "r")

    split_groups(dataset, pre)

    return {"statusCode": 200, "body": "Complete"}
