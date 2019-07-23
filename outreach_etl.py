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

from gmail_tools import SendMessageWithAttachment

from google.oauth2 import service_account
from google.cloud import bigquery

# BigQuery credentials
cred_path = '/Users/christianbramwell/Documents/Turn:River Capital/Coding Scripts/google-cloud-credentials/'
service_cred = cred_path + "turn-river-capital-5af901fddf11.json"

# Logging
log_filename = 'log-outreach-script.log'
file_handler = logging.FileHandler(filename=log_filename, mode='w')
stdout_handler = logging.StreamHandler(sys.stdout)
handlers = [file_handler, stdout_handler]
format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(handlers=handlers, level=logging.INFO, format=format, datefmt='%d-%b-%y %H:%M:%S')

# ---------------------------------------------------------------------------- #

# Parse Arguments

def load_json(path):
    with open(path) as fil:
        return json.load(fil)
'''
parser = argparse.ArgumentParser()

parser.add_argument(
    '-c', '--config',
    help='Config file',
    required=True)

args = parser.parse_args()
config = load_json(args.config)
'''
config = load_json('acunetix_creds.json')

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
    if endpoint == 'mailings':
        json_norm.pop('attributes_bodyHtml')
        json_norm.pop('attributes_bodyText')
    dF = pd.DataFrame(data=json_norm)
    return {"data": dF, "json_data": json_data}

def sync(endpoint, page_size, date_list):

    # Get acess token using the refresh token
    access_token = get_access_token()['access_token']
    response_dF = pd.DataFrame(columns=outreach_api(endpoint, access_token)['data'].columns)

    for date in date_list:

        querystring = {'sort': '-updatedAt',
                        'page[limit]': str(page_size),
                        'filter[updatedAt]': date + ".." + date}

        logging.info('Getting {} for {}'.format(endpoint, date))
        response = outreach_api(endpoint, access_token, querystring=querystring) # get the columns of the prospects table
        temp_dF = response['data'] # create temporary DataFrame
        response_dF = pd.concat([response_dF, temp_dF], axis=0, ignore_index=True, sort=False)

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

    response_dF = response_dF.infer_objects()
    # file_date = str(datetime.today().date())
    # filename = "{} outreach_{}.csv".format(file_date, endpoint)
    # response_dF.to_csv(filename, index=False)
    bigquery_upload(config['project'], config['dataset'], config['table'], response_dF)

def bigquery_upload(project_name, dataset_name, table_name, dF):

    credentials = service_account.Credentials.from_service_account_file(
        service_cred)

    # load dataframe into BigQuery
    client = bigquery.Client(project=project_name, credentials=credentials)
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)

    client.load_table_from_dataframe(dF, table_ref).result()

    logging.info("Completed BigQuery Upload")

# ---------------------------------------------------------------------------- #

if config['replication_type'] == 'full':
    min_date = datetime.strptime(config['start_date'], "%Y-%m-%d").date()
    max_date = (datetime.now() - timedelta(days = 1)).date()

else:
    min_date = (datetime.now() - timedelta(days = 1)).date()
    max_date = (datetime.now() - timedelta(days = 1)).date()

page_size = 100

delta = max_date - min_date

date_list = [(min_date + timedelta(i)).strftime("%Y-%m-%d") for i in range(delta.days + 1)]

sync('prospects', page_size, date_list)
sync('sequences', page_size, date_list)
sync('mailings', page_size, date_list)

# Send log file via email
sender = 'me'
to = config['email']
subject = config['table'] + ' - Outreach ETL Log'
message_text = 'This is the log file for the Outreach ETL tool.'
file_dir = os.getcwd()
SendMessageWithAttachment(sender, to, subject, message_text, file_dir, log_filename)
