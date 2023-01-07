import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection,AWSV4SignerAuth
import csv

# Reference: https://docs.aws.amazon.com/opensearch-service/latest/developerguide/search-example.html
# Reference: https://docs.aws.amazon.com/lambda/latest/dg/python-package.html

region = 'us-east-1'
credentials = boto3.Session().get_credentials()
auth = AWSV4SignerAuth(credentials, region)

host = 'search-restaurants-6vnek5tpsoommj6acyiww2vdue.us-east-1.es.amazonaws.com'
index = 'restaurants'
url = host + '/' + index + '/restaurant'

# Lambda execution starts here
def lambda_handler(event, context):
    es = OpenSearch(
        hosts=[{'host': host, 'port': 443}],
        use_ssl=True,
        verify_certs=True,
        http_auth=auth,
        connection_class=RequestsHttpConnection
    )
    # Elasticsearch 6.x requires an explicit Content-Type header
    headers = { "Content-Type": "application/json" }

    with open('restaurant.csv', newline='') as file:
        reader = csv.reader(file)
        restaurants = list(reader)
        restaurants = restaurants[1:]
    for r in restaurants:
        body = {
            'RestaurantID': r[0],
            'Cuisine': r[3]
        }
        es.index(index="restaurants", id=r[0], body=body)
