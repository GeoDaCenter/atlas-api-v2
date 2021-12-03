from datetime import datetime, timedelta, date
import json

from google.oauth2 import service_account
from google.cloud import bigquery

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
        path = event["path"]
        param = event["queryStringParameters"]
        query = build_query_by_parsing(path, param)
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

def build_query_by_parsing(path, param):
    '''
    Test example:
    path is required, param is optional
    path = '/vaccination/county/cumulative',
    param = {'source': 'cdc',
           'from': '2020-05-01',
           'geoid': '(1131,204031,206063,206091)',
           'to': '2020-11-01'}
    '''
    path_lst = path.split('/')
    dataset, spatial_unit, temporal_unit = [i for i in path_lst[1:]]
    
    select_clause = create_select_clause(temporal_unit, param)
    
    # default data source is cdc
    if "source" in param:
        from_clause = create_from_clause(path, param["source"])
    else:
        from_clause = create_from_clause(path, "cdc") 
    
    query = select_clause  + from_clause
    
    if "geoid" in param:
        where_clause = f" WHERE fips_code in {param['geoid']}"
        query += where_clause

    return query

def create_from_clause(path, source):
    '''
    source: datasource
    
    '''
    path_lst = path.split('/')
    dataset, spatial_unit, temporal_unit = [i for i in path_lst[1:]]
    
    from_clause = " FROM covid-atlas.public."
    
    if dataset == "vaccination":
        from_clause += ("vaccination_fully_vaccinated" + f"_{source}")
    elif dataset == "confirmed":
        from_clause += ("covid_confirmed" + f"_{source}")
    elif dataset == "deaths":
        from_clause += ("covid_deaths" + f"_{source}")
    
    if spatial_unit == "state":
        from_clause += "_state"
        
    return from_clause

def create_select_clause(temporal_unit, param):
    '''
    Input:
    temporal_unit(str): cumulative, time-series, snapshot
    param(dic): queryStringParameters
    Rules:
    if temporal_unit is snapshot, then 'recentX' (recent x days) in param
    if "from" in param, "to" is in param
    if temporal_unit is snapshot, it's stronger than "from" and "to"
    '''
    
    select_clause = "SELECT fips_code"
    
    if temporal_unit == "snapshot":
        end = date.today()
        start = end - timedelta(days=param['recentX'])
        date_list = [(start + timedelta(days=x)).strftime("_%Y_%m_%d") for x in range((end - start).days)]
        columns = (', ').join(date_list)
        select_clause += f", {columns}"
        
    elif "from" in param:
        start = datetime.fromisoformat(param["from"])
        end = datetime.fromisoformat(param["to"])
        date_list = [(start + timedelta(days=x)).strftime("_%Y_%m_%d") for x in range((end - start).days)]
        columns = (', ').join(date_list)
        select_clause += f", {columns}"
        
    return select_clause    

