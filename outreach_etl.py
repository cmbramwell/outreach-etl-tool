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

from etl_tools import bigquery_upload
from etl_tools import SendMessageWithAttachment
from etl_tools import flatten_json

from google.oauth2 import service_account
from google.cloud import bigquery

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

cred_path = config['cred_path']
service_cred = cred_path + "turn-river-capital-5af901fddf11.json"
token_expires = None

def get_access_token():

    url = "https://api.outreach.io/oauth/token"

    querystring = {
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
            "redirect_uri": config["redirect_uri"],
            "grant_type": "refresh_token",
            "refresh_token": config["refresh_token"]
            }
    payload = ""
    headers = {
        }

    response = requests.request("POST", url, data=payload, headers=headers, params=querystring)
    token_expires = datetime.now() + timedelta(seconds=7200 - 100)
    return response.json()

def request(endpoint, querystring=None, next_page_url=None):

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

    if token_expires is None or datetime.now() > token_expires:
        global access_token
        access_token = get_access_token()['access_token']

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

def sync(endpoint, page_size, min_date, max_date):

    querystring = {'sort': '-updatedAt',
                    'page[limit]': str(page_size),
                    'filter[updatedAt]': min_date.strftime("%Y-%m-%d") + ".." + max_date.strftime("%Y-%m-%d")}

    response = request(endpoint, querystring=querystring)
    response_dF = pd.DataFrame(columns=response['data'].columns)
    num_responses = response['json_data']['meta']['count']

    if num_responses == 0:

        logging.info('There are no {}'.format(endpoint))
        return

    elif num_responses <= 10000:

        logging.info('Number of {} is {}'.format(endpoint, num_responses))
        num_pages = math.ceil(num_responses / page_size)
        logging.info('Completed Page 1 out of {}'.format(num_pages))

        if num_pages > 1:

            count = 1
            while 'next' in response['json_data']['links']:

                complete = None
                while complete == None:

                    try:
                        next_page_url = response['json_data']['links']['next']
                        response = request(endpoint, next_page_url=next_page_url)

                    except:
                        logging.error('There was an API error.')

                    else:
                        count += 1
                        complete = True

                    finally:
                        temp_dF = response["data"]
                        response_dF = pd.concat([response_dF, temp_dF], axis=0, ignore_index=True, sort=False)
                        logging.info('Completed Page {} out of {}'.format(count, num_pages))

    else:

        delta = max_date - min_date
        date_list = [(min_date + timedelta(i)).strftime("%Y-%m-%d") for i in range(delta.days + 1)]

        for date in date_list:

            querystring = {'sort': '-updatedAt',
                            'page[limit]': str(page_size),
                            'filter[updatedAt]': date + ".." + date}

            logging.info('Getting {} for {}'.format(endpoint, date))

            complete = None
            while complete == None:

                try:
                    response = request(endpoint, querystring=querystring) # get the columns of the prospects table

                except:
                    logging.error('There was an API error.')

                else:
                    complete = True

            temp_dF = response['data'] # create temporary DataFrame
            response_dF = pd.concat([response_dF, temp_dF], axis=0, ignore_index=True, sort=False)

            # Get the number of prospects and number of pages
            num_responses = response['json_data']['meta']['count']
            logging.info('Number of {} is {}'.format(endpoint, num_responses))
            num_pages = math.ceil(num_responses / page_size)
            logging.info('Completed Page 1 out of {}'.format(num_pages))

            if num_pages > 1:

                count = 1
                while 'next' in response['json_data']['links']:

                    complete2 = None
                    while complete2 == None:

                        try:
                            next_page_url = response['json_data']['links']['next']
                            response = request(endpoint, next_page_url=next_page_url)

                        except:
                            logging.error('There was an API error.')

                        else:
                            count += 1
                            complete2 = True

                        finally:
                            temp_dF = response["data"]
                            response_dF = pd.concat([response_dF, temp_dF], axis=0, ignore_index=True, sort=False)
                            logging.info('Completed Page {} out of {}'.format(count, num_pages))

    response_dF.columns = response_dF.columns.str.replace('attributes_', '')
    response_dF.createdAt = pd.to_datetime(response_dF.createdAt)
    response_dF.updatedAt = pd.to_datetime(response_dF.updatedAt)
    response_dF = response_dF.infer_objects()
    table_name = config['table'] + '_' + endpoint
    bigquery_upload(service_cred, config['project'], config['dataset'], table_name, response_dF)
    logging.info("Completed BigQuery Upload")

# ---------------------------------------------------------------------------- #

if config['replication_type'] == 'full':
    min_date = datetime.strptime(config['start_date'], "%Y-%m-%d").date()
    max_date = (datetime.now() - timedelta(days = 1)).date()

else:
    min_date = (datetime.now() - timedelta(days = 1)).date()
    max_date = (datetime.now() - timedelta(days = 1)).date()

page_size = 100

#sync('prospects', page_size, min_date, max_date)
#sync('sequences', page_size, min_date, max_date)
sync('mailings', page_size, min_date, max_date)

# Send log file via email
sender = 'me'
to = config['email']
subject = 'Outreach ETL Log - ' + config['table']
message_text = 'This is the log file for the Outreach ETL tool.'
file_dir = os.getcwd()
SendMessageWithAttachment(cred_path, sender, to, subject, message_text, file_dir, log_filename)
