import json
import os

dir_path = os.path.dirname(os.path.realpath(__file__)) # /path-to-repo/covid/data-scripts/bigquery
repo_root = os.path.abspath(os.path.join(dir_path, '..')) # /path-to-repo/covid/

def build_creds():
    return {
        "type":"service_account",
        "project_id":"covid-atlas",
        "private_key_id":os.getenv('SK_ID'),
        "private_key":os.getenv('SK').replace('\\\\n', '\n').replace('\\n', '\n'),
        "client_email":os.getenv('G_CLIENT_EMAIL'),
        "client_id":os.getenv('G_ID'),
        "auth_uri":"https://accounts.google.com/o/oauth2/auth",
        "token_uri":"https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url":"https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url":os.getenv('G_CERT_URL')
    }

if __name__ == '__main__':
    credentials = build_creds();
    with open(os.path.join(repo_root, 'atlas_api/credentials.json'), 'w') as f:
        json.dump(credentials, f)