from google.oauth2 import service_account
from google.cloud import bigquery
import json
# import pandas as pd

def getClient():
    return bigquery.Client(credentials=service_account.Credentials.from_service_account_file('credentials.json'))

def queryGbq(client, query):
    query_job = client.query(query)
    return query_job.result().to_dataframe(create_bqstorage_client=False)

def lambda_handler(event, context):
    """Sample pure Lambda function
    """

    try:
        client = getClient()
        query = 'SELECT * FROM `covid-atlas.public.chr_life` LIMIT 100'
        results = queryGbq(client, query)
        if True:
            formatted_results = results.to_json(orient='records')

    except Exception as e:
        # Send some context about this error to Lambda Logs
        print(e)

    # print(results.head())
    return {
        "statusCode": 200,
        "body": json.dumps({
            "data": formatted_results
        }),
    }