import sqlite3
import sys
import logging
import os
import zlib
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


def mbtiles_to_disk(mbtiles_file, loc, **kwargs):
    con = mbtiles_connect(mbtiles_file)

    metadata = dict(con.execute('select name, value from metadata;').fetchall())
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
            y = t[2]
            """Push T file to DynamoDB"""
            key = str(loc + "-" + str(z) + "-" + str(y) + "-" + str(x))
            entry = {}
            entry["tileKey"] = key
            entry["tile"] = t[3]
            entry["huge"] = len(t[3]) > 400000
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
