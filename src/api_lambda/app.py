import os
import json
import requests

def lambda_handler(event, context):
    try:
        query = event.get("gremlin_query", "g.V().limit(1)")
        print(f"Executing Gremlin query: {query}")

        neptune_http_endpoint = os.environ['NEPTUNE_HTTP_ENDPOINT']

        response = requests.post(
            neptune_http_endpoint,
            headers={"Content-Type": "application/json"},
            data=json.dumps({"gremlin": query}),
        )

        return {
            'statusCode': response.status_code,
            'body': response.text
        }

    except Exception as e:
        print(f"Error querying Neptune: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }