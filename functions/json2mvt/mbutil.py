import sqlite3
import sys
import logging
import os
import json
import math
import boto3

"""
Code Adapted from MBUTIL
@author github:@StreamlinesUNH
Modified into a cloud-native SQL Egress via Lambda to AWS DynamoDB
"""

dynamodb = boto3.resource("dynamodb")
dynamodb_table = dynamodb.Table(os.getenv("DATA_TABLE"))
huge_bucket = os.getenv("DATA_BUCKET")
s3 = boto3.client("s3")

logger = logging.getLogger(__name__)


def flip_y(zoom, y):
    return (2 ** zoom - 1) - y

def mbtiles_connect(mbtiles_file):
    try:
        con = sqlite3.connect(mbtiles_file)
        print("Grabbed SQLite Connction")
        return con
    except Exception as e:
        logger.error("Could not connect to database")
        logger.exception(e)
        sys.exit(1)


def mbtiles_to_disk(mbtiles_file, loc, update_time, **kwargs):
    con = mbtiles_connect(mbtiles_file)

    # metadata = dict(con.execute('select name, value from metadata;').fetchall())
    # json.dump(metadata, open(os.path.join(directory_path, 'metadata.json'), 'w'), indent=4)

    count = con.execute('select count(zoom_level) from tiles;').fetchone()[0]
    print(str(count) + " Tiles Generate!\n")

    """ADD STYLING LAYER HERE, RESEARCH JSON FORMATTER AND LAYER_JSON"""
    # formatter = metadata.get('formatter')
    # if formatter:
    #     layer_json = os.path.join(base_path, 'layer.json')
    #     formatter_json = {"formatter":formatter}
    #     open(layer_json, 'w').write(json.dumps(formatter_json))

    """Grab Tiles and Process to DynamoDB"""
    tiles = con.execute('select zoom_level, tile_column, tile_row, tile_data from tiles;')
    t = tiles.fetchone()
    with dynamodb_table.batch_writer() as batch:
        while t:
            z = t[0]
            x = t[1]
            y = flip_y(z, t[2])
            """Push T file to DynamoDB"""
            key = str(loc + "-" + str(z) + "-" + str(x) + "-" + str(y))
            entry = {}
            entry["tileKey"] = key
            entry["tile"] = t[3]
            entry["huge"] = len(t[3]) > 400000
            entry["timestamp"] = update_time
            if not entry["huge"]:
                batch.put_item(Item=entry)
            else:
                print("Miss:", key, len(t[3]))
                s3.put_object(
                        Bucket=huge_bucket,
                        Key=key,
                        Body=t[3])
                entry["tile"] = str.encode("a") #need non-null
                batch.put_item(Item=entry)

            t = tiles.fetchone()
def angle3pt(a, b, c):
    ang = math.degrees(math.atan2(c[1]-b[1], c[0]-b[0]) - math.atan2(a[1]-b[1], a[0]-b[0]))
    return ang + 360 if ang < 0 else ang


def json_filter(obj):
    new_obj = {"type": "FeatureCollection", "bbox": [], "features": []}

    new_obj["bbox"] = obj["bbox"]
    for x in range(0, len(obj["features"])):
        feat = {
                "type": "Feature",
                "id": x,
                "properties": {},
                "geometry": {"type": "LineString", "properties": [], "coordinates": []},
                }
        if len(obj["features"][x]["geometry"]["coordinates"]) <= 4:
            continue
        nline = []
        nProps = {
                "index": obj["features"][x]["properties"]["index"],
                "streamline_level": obj["features"][x]["properties"]["streamline_level"],
                "seed_index": obj["features"][x]["properties"]["seed_index"],
                "point_levels":[],
                "magnitudes":[],
                "directions": [],
                "dSep": obj["features"][x]["properties"]["dSep"],
                "iSteps": obj["features"][x]["properties"]["iSteps"]
                }
        skip = False
        m1 = len(obj["features"][x]["geometry"]["coordinates"]) - 2
        for y in range(0, m1):
            cords = obj["features"][x]["geometry"]["coordinates"][y]
            cnext = obj["features"][x]["geometry"]["coordinates"][y + 1]
            cnext1 = obj["features"][x]["geometry"]["coordinates"][y + 2]
            angle = angle3pt(cords, cnext, cnext1)
            if not skip:
                nProps["magnitudes"].append(obj["features"][x]["properties"]["magnitudes"][y])
                nProps["directions"].append(obj["features"][x]["properties"]["directions"][y])
                nProps["point_levels"].append(obj["features"][x]["properties"]["point_levels"][y])
                feat["geometry"]["coordinates"].append(cords)
            skip = (angle > 270)
        if not skip:
            nProps["magnitudes"].append(obj["features"][x]["properties"]["magnitudes"][m1+1])
            nProps["directions"].append(obj["features"][x]["properties"]["directions"][m1+1])
            nProps["point_levels"].append(obj["features"][x]["properties"]["point_levels"][m1+1])
            feat["geometry"]["coordinates"].append(obj["features"][x]["geometry"]["coordinates"][m1+1])
        feat["geometry"]["properties"] = nProps
        new_obj["features"].append(feat)
    return new_obj
