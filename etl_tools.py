import json
import numpy as np
import pandas as pd

import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

from apiclient import discovery
from apiclient import errors
from httplib2 import Http

from google.oauth2 import service_account
from google.cloud import bigquery

import base64
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import mimetypes
import os

from apiclient import errors

# ---------------------------------------------------------------------------- #

# SCHEMA Functions

def load_json(path):
    with open(path) as fil:
        return json.load(fil)

def create_dF_from_schema(endpoint):

    schema = load_json("schemas/" + endpoint + ".json")

    dF = pd.DataFrame(columns=list(schema.keys()))

    for col, dtype in schema.items():

        dtype = dtype['type'][1]
        if dtype == 'string' or dtype == 'boolean':
            continue
        if dtype == 'integer':
            dF[col] = pd.to_numeric(dF[col], downcast='integer')
        if dtype == 'float':
            dF[col] = pd.to_numeric(dF[col], downcast='float')
        if dtype == 'datetime':
            dF[col] = pd.to_datetime(dF[col])

    return dF

# ---------------------------------------------------------------------------- #

# BIGQUERY Functions

def bigquery_upload(service_cred, project_name, dataset_name, table_name, dF):

    credentials = service_account.Credentials.from_service_account_file(
        service_cred)

    # load dataframe into BigQuery
    client = bigquery.Client(project=project_name, credentials=credentials)
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)

    client.load_table_from_dataframe(dF, table_ref).result()

# ---------------------------------------------------------------------------- #

# GMAIL Functions

def google_auth(cred_path):

    SCOPES = ['https://www.googleapis.com/auth/gmail.send']

    creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
    if os.path.exists(cred_path + 'token.pickle'):
        with open(cred_path + 'token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                cred_path + 'credentials_2.json', SCOPES)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open(cred_path + 'token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    GMAIL = discovery.build('gmail', 'v1', credentials=creds, cache_discovery=False)
    return GMAIL

def SendMessageWithAttachment(cred_path, sender, to, subject, message_text, file_dir, filename):

    GMAIL = google_auth(cred_path)

    """Create a message for an email.

    Args:
    sender: Email address of the sender.
    to: Email address of the receiver.
    subject: The subject of the email message.
    message_text: The text of the email message.
    file_dir: The directory containing the file to be attached.
    filename: The name of the file to be attached.

    Returns:
    An object containing a base64url encoded email object.
    """
    message = MIMEMultipart()
    message['to'] = to
    message['from'] = sender
    message['subject'] = subject

    msg = MIMEText(message_text)
    message.attach(msg)

    path = os.path.join(file_dir, filename)
    content_type, encoding = mimetypes.guess_type(path)

    if content_type is None or encoding is not None:
        content_type = 'application/octet-stream'
    main_type, sub_type = content_type.split('/', 1)
    if main_type == 'text':
        fp = open(path, 'rb')
        msg = MIMEText(fp.read(), _subtype=sub_type, _charset='utf-8')
        fp.close()
    elif main_type == 'image':
        fp = open(path, 'rb')
        msg = MIMEImage(fp.read(), _subtype=sub_type, _charset='utf-8')
        fp.close()
    elif main_type == 'audio':
        fp = open(path, 'rb')
        msg = MIMEAudio(fp.read(), _subtype=sub_type, _charset='utf-8')
        fp.close()
    else:
        fp = open(path, 'rb')
        msg = MIMEBase(main_type, sub_type)
        msg.set_payload(fp.read())
        fp.close()

    msg.add_header('Content-Disposition', 'attachment', filename=filename)
    message.attach(msg)

    b64_bytes = base64.urlsafe_b64encode(message.as_bytes())
    b64_string = b64_bytes.decode()
    body = {'raw': b64_string}

    GMAIL.users().messages().send(userId='me', body=body).execute()

    return {'raw': body}

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

# ---------------------------------------------------------------------------- #
