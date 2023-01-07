from __future__ import print_function

import argparse
import json
import pprint
import requests
import sys
import boto3
import datetime

from urllib.parse import quote
from decimal import *


API_KEY= '3zAU2zmCrg1qBiM8PosqzlAN6yFbKyxPjUk0My2AOgcYdIAyd73LJBoTowDafXwZ02v8oQeoOxJ0JpzFKE5pxMXyHpUmJ8IuKPy6dZAbuQE9MYQ19jEU61CyOERCY3Yx'


# API constants, you shouldn't have to change these.
API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  # Business ID will come after slash.
TOKEN_PATH = '/oauth2/token'
GRANT_TYPE = 'client_credentials'

# Defaults for our simple example.
DEFAULT_TERM = 'dinner'
DEFAULT_LOCATION = 'Manhattan'
SEARCH_LIMIT = 1000

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-restaurants')

restaurants = {}

def request(host, path, api_key, url_params=None):

    url_params = url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }

    print(u'Querying {0} ...'.format(url))

    response = requests.request('GET', url, headers=headers, params=url_params)

    return response.json()


def search(api_key, term, page_off):

    url_params = {
        'term': term,
        'location': DEFAULT_LOCATION,
        'offset': page_off,
        'limit': 50,
        'sort_by': 'rating'
    }
    return request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)


def empty_replace(input):
    if len(str(input)) == 0:
        return 'N/A'
    else:
        return input

def addItems(data, cuisine):
    global restaurants
    for info in data:
        if info['id'] in restaurants:
            continue
        table.put_item(
            Item={
                'Business ID': empty_replace(info['id']),
                'insertedAtTimestamp': empty_replace(str(datetime.datetime.now())),
                'Name': empty_replace(info['name']),
                'Cuisine': empty_replace(cuisine),
                'Rating': empty_replace(Decimal(info['rating'])),
                'Number of Reviews': empty_replace(Decimal(info['review_count'])),
                'Address': empty_replace(info['location']['address1']),
                'Zip Code': empty_replace(info['location']['zip_code']),
                'Latitude': empty_replace(Decimal(str(info['coordinates']['latitude']))),
                'Longitude': empty_replace(Decimal(str(info['coordinates']['longitude'])))
            }
        )

def query_api():

    cuisine = ['italian', 'chinese', 'indian', 'korean', 'thai', 'american', 'mexican', 'spanish', 'greek', 'latin', 'Persian']
    for c in cuisine:
        page_off = 0
        while page_off < SEARCH_LIMIT:
            # print(c+" restaurants")
            js = search(API_KEY, c+" restaurants", page_off)
            if js.get('businesses'):
                addItems(js['businesses'], c)
            page_off += 50

def lambda_handler(event, context):
    query_api()
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': API_HOST,
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps('Hello from Lambda!')
    }