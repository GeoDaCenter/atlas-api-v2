from google.oauth2 import service_account
from google.cloud import bigquery
import json

def getClient():
    return bigquery.Client(credentials=service_account.Credentials.from_service_account_file('atlas_api/credentials.json'))

def queryGbq(client, query):
    query_job = client.query(query)
    return query_job.result()

def lambda_handler(event, context):
    """Sample pure Lambda function

    """

    try:
        client = getClient()
        query = 'SELECT * FROM `covid-atlas.public.chr_life` LIMIT 100'
        results = queryGbq(client, query)
        
    except Exception as e:
        # Send some context about this error to Lambda Logs
        print(e)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "data": results
        }),
    }
