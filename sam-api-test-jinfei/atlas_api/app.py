from google.oauth2 import service_account
from google.cloud import bigquery
import json
from urllib.parse import urlparse
import re
from datetime import datetime, timedelta

# import pandas as pd

def getClient(path='credentials.json'):
    return bigquery.Client(credentials=service_account.Credentials.from_service_account_file(path))

def queryGbq(client, query):
    query_job = client.query(query)
    return query_job.result().to_dataframe(create_bqstorage_client=False)

def lambda_handler(event, context):
    """Sample pure Lambda function
    """

    try:
        client = getClient(path='credentials.json')
        query = build_query_by_parsing(event['requestContext']['path'])
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

def build_query_by_parsing(url):
    '''
    Test example:
    dataset = 'vaccination'
    spatial_unit = 'county'
    temporal_unit = 'cumulative'
    date_range_filter = '?from=2020-05-01&to=2020-11-01'
    geoid_filter = '?geoid=(1131, 4031, 6063, 6091)'
    url = f'api.uscovidatlas.org/{dataset}/{spatial_unit}/{temporal_unit}{date_range_filter}{geoid_filter}'
    '''
    parsed = urlparse(url)
    path = parsed.path
    # split the path into list, like: ['api.uscovidatlas.org', 'vaccination', 'county', 'cumulative']
    path_lst = parsed.path.split('/')
    dataset, spatial_unit, temporal_unit = [i for i in path_lst[1:]]
    
    # split the query into list
    filter_lst = parsed.query.split('?')
    for i in filter_lst:
        if i.startswith('from'):
            pattern = r'(\d{4}-\d{2}-\d{2})'
            start, end = [datetime.fromisoformat(t) for t in re.findall(pattern, i)]
            
            date_list = [(start + timedelta(days=x)).strftime("_%Y_%m_%d") for x in range((end - start).days)]
            columns = (', ').join(date_list)

        if i.startswith('geoid'):
            # from format 'geoid=(1131, 4031, 6063, 6091)']
            geo_lst = i[6:]
    
    sql = f"""
            SELECT  fips, {columns}
            FROM `covid-atlas.public.{dataset}_{spatial_unit}_{temporal_unit}`
            WHERE fips in {geo_lst} 
            """
    return sql




# # for local testing
# if __name__ == "__main__":
#     client = getClient(path='atlas_api/credentials.json')
#     query = 'SELECT * FROM `covid-atlas.public.chr_life` LIMIT 100'
#     results = queryGbq(client, query)
#     print(results.head())