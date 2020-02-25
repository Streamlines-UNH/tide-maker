import json


def lambda_handler(event, context):
    URL_BASE = ("https://" + event["headers"]["Host"] +
                "/" + event["requestContext"]["stage"])
    data = [
        {
            "URL": URL_BASE + "/api/CBOFS/{z}/{x}/{y}.pbf",
            "location": [-75.5, 37.5],
            "min-zoom": 0,
            "max-zoom": 14,
            "time-min": 0,
            "time-max": 48,
            "fullname": "Chesapeake Bay Operational Forecast System",
            "shortname": "CBOFS",
            "avaliable_predictions": 48,
            "date": 23,
            "prediction_timesteps": 1,
        },
        {
            "URL": URL_BASE + "/api/DBOFS/{z}/{x}/{y}.pbf",
            "location": [-72.5, 42.5],
            "min-zoom": 0,
            "max-zoom": 14,
            "time-min": 0,
            "time-max": 48,
            "fullname": "Delaware Bay Operational Forecast System",
            "shortname": "CBOFS",
            "avaliable_predictions": 48,
            "date": 23,
            "prediction_timesteps": 1,
        },
        {
            "URL": URL_BASE + "/api/NYOFS/{z}/{x}/{y}.pbf",
            "location": [-70, 38.5],
            "min-zoom": 0,
            "max-zoom": 14,
            "time-min": 0,
            "time-max": 48,
            "fullname": "New York and New Jersey Operational Forecast System",
            "shortname": "NYOFS",
            "avaliable_predictions": 48,
            "date": 23,
            "prediction_timesteps": 1,
        }
    ]
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(data)
    }
