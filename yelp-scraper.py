from __future__ import print_function

import argparse
import json
import pprint
import requests
import sys
import urllib
import datetime
from urllib.parse import quote

from decimal import *
import csv


API_KEY= '3zAU2zmCrg1qBiM8PosqzlAN6yFbKyxPjUk0My2AOgcYdIAyd73LJBoTowDafXwZ02v8oQeoOxJ0JpzFKE5pxMXyHpUmJ8IuKPy6dZAbuQE9MYQ19jEU61CyOERCY3Yx'



API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  
TOKEN_PATH = '/oauth2/token'
GRANT_TYPE = 'client_credentials'

DEFAULT_TERM = 'dinner'
DEFAULT_LOCATION = 'Manhattan'
SEARCH_LIMIT = 1000

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
    log_path = 'restaurant.csv'
    file = open(log_path, 'a+', encoding='utf-8', newline='')
    csv_writer = csv.writer(file)
    csv_writer.writerow([f'Business ID','insertedAtTimestamp','Name','Cuisine','Rating',
                         'Number of Reviews','Address','Zip Code','Latitude','Longitude'])
    for info in data:
        if info["alias"] in restaurants:
            continue
        csv_writer.writerow([
                empty_replace(info['id']),
                empty_replace(str(datetime.datetime.now())),
                empty_replace(info['name']),
                empty_replace(cuisine),
                empty_replace(Decimal(info['rating'])),
                empty_replace(Decimal(info['review_count'])),
                empty_replace(info['location']['address1']),
                empty_replace(info['location']['zip_code']),
                empty_replace(Decimal(str(info['coordinates']['latitude']))),
                empty_replace(Decimal(str(info['coordinates']['longitude'])))
            ])
    file.close()

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
        'body': json.dumps('Hello from Lambda!')
    }