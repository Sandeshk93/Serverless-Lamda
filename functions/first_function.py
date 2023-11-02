import json

def first_function(event, context):
    # Handle GET request
    response = {
        "statusCode": 200,
        "body": json.dumps({"message": "This is a GET request"})
    }
    return response
