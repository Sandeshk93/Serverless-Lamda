import json

def second_function(event, context):
    # Handle POST request
    try:
        # Parse the request body
        request_body = json.loads(event['body'])
        
        # Do something with the request data
        # For example, you can process the data and store it in a database
        
        response = {
            "statusCode": 200,
            "body": json.dumps({"message": "This is a POST request", "data_received": request_body})
        }
    except Exception as e:
        # Handle exceptions, if any
        response = {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
    
    return response
