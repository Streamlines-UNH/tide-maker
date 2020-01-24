import os


def lambda_handler(event, context):
    # Right now this just returns the datapath of the tile that was
    # requested as it will be stored in dynamodb
    table_index = "{}/{}/{}/{}".format(
        event["pathParameters"]["region"],
        event["pathParameters"]["z"],
        event["pathParameters"]["x"],
        os.path.splitext(event["pathParameters"]["y"])[0]
    )
    return {
        'statusCode': 200,
        'body': table_index
    }
