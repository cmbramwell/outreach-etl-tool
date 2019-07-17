# ---------------------------------------------------------------------------- #

'''

Outreach API Calls

By: Christian Bramwell
Goal: This script will collect Outreach API data and upload to BigQuery

Outreach - https://api.outreach.io/api/v2/docs

'''

# ---------------------------------------------------------------------------- #

# Import libraries

import sys
import logging
import time
from datetime import datetime, timedelta, date
import os
import math
import re
import argparse

import numpy as np
import pandas as pd
import requests
from pandas.io.json import json_normalize
import json

from google.cloud import storage
from google.cloud import bigquery

file_handler = logging.FileHandler(filename='log_outreach_script.log', mode='w')
stdout_handler = logging.StreamHandler(sys.stdout)
handlers = [file_handler, stdout_handler]

format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(handlers=handlers, level=logging.INFO, format=format, datefmt='%d-%b-%y %H:%M:%S')

# ---------------------------------------------------------------------------- #

# Parse Arguments

def load_json(path):
    with open(path) as fil:
        return json.load(fil)

parser = argparse.ArgumentParser()

parser.add_argument(
    '-c', '--config',
    help='Config file',
    required=True)

args = parser.parse_args()
config = load_json(args.config)

# ---------------------------------------------------------------------------- #

# Flatten JSON

def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def get_access_token():

    url = "https://api.outreach.io/oauth/token"

    querystring = {
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
            "redirect_uri": config["redirect_uri"],
            "grant_type": "refresh_token",
            "refresh_token": config["refresh_token"]}

    payload = ""
    headers = {
        }

    response = requests.request("POST", url, data=payload, headers=headers, params=querystring)
    return response.json()

def outreach_api(endpoint, access_token, querystring=None, next_page_url=None):

    if endpoint == 'prospects':
        url = 'https://api.outreach.io/api/v2/prospects'
    elif endpoint == 'sequences':
        url = 'https://api.outreach.io/api/v2/sequences'
    elif endpoint == 'mailings':
        url = 'https://api.outreach.io/api/v2/mailings'
    elif endpoint == 'accounts':
        url = 'https://api.outreach.io/api/v2/accounts'
    elif endpoint == 'opportunities':
        url = 'https://api.outreach.io/api/v2/opportunities'

    payload = ""
    headers = {
        'Content-Type': "application/vnd.api+json",
        'Authorization': "Bearer " + access_token
        }

    if next_page_url is None:
        response = requests.request("GET", url, data=payload, headers=headers, params=querystring)

    else:
        response = requests.request("GET", next_page_url, data=payload, headers=headers)

    json_data = response.json()
    json_flat = [flatten_json(i) for i in json_data["data"]]
    json_norm = json_normalize(json_flat)
    dF = pd.DataFrame(data=json_norm)
    return {"data": dF, "json_data": json_data}

def sync(endpoint, page_size, querystring=None):

    # Get acess token using the refresh token
    access_token = get_access_token()['access_token']

    response = outreach_api(endpoint, access_token, querystring=querystring) # get the columns of the prospects table
    response_dF = response['data'] # create empty dataframe

    # Get the number of prospects and number of pages
    num_responses = response['json_data']['meta']['count']
    logging.info('Number of {} is {}'.format(endpoint, num_responses))
    num_pages = math.ceil(num_responses / page_size)
    logging.info('Completed Page 1 out of {}'.format(num_pages))

    if num_pages > 1:

        count = 2

        while 'next' in response['json_data']['links']:

            next_page_url = response['json_data']['links']['next']
            response = outreach_api(endpoint, access_token, next_page_url=next_page_url)

            # Add prospects to dataframe
            temp_dF = response["data"]
            response_dF = pd.concat([response_dF, temp_dF], axis=0, ignore_index=True, sort=False)

            logging.info('Completed Page {} out of {}'.format(count, num_pages))
            count += 1

    file_date = str(datetime.today().date() - timedelta(days=1))
    filename = "{} outreach_{}.csv".format(file_date, endpoint)
    response_dF.to_csv(filename, index=False)

# ---------------------------------------------------------------------------- #

# Set min and max dates for querystring
min_date = date(2019, 7, 15) # set minimum pull date
max_date = (datetime.now() - timedelta(days = 1)).date() # set maximum pull date

page_size = 100

querystring = {'sort': '-createdAt',
                'page[limit]': str(page_size),
                'filter[createdAt]': min_date.strftime("%Y-%m-%d") + ".." + max_date.strftime("%Y-%m-%d")}

sync('prospects', page_size, querystring=querystring)
sync('sequences', page_size)
sync('mailings', page_size, querystring=querystring)

# ---------------------------------------------------------------------------- #
'''
# Google Cloud

from google.oauth2 import service_account
import pandas_gbq

service_cred = "turn-river-capital-81ffa9ec748e.json"

credentials = service_account.Credentials.from_service_account_file(
    service_cred)

prospects_dF.to_gbq(destination_table="netsparker.outreach_prospects",
                        if_exists="replace",
                        credentials=credentials)

logging.info("Completed BigQuery Upload for Prospects")
'''
