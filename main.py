import boto3
import csv
import json
import requests
import logging
import yaml



def fetch_config(name):
  with open('config/'+name+'.yml') as config_file_main:
    return yaml.load(config_file_main, Loader=yaml.BaseLoader)


def fetch_zendesk_users(endpoint, username, password):
  zendesk_api_auth = (username, password)
  zendesk_api_response = requests.get(endpoint, auth=zendesk_api_auth)
  if zendesk_api_response.status_code != 200:
    raise ValueError('ZenDesk returned non-200 status', zendesk_api_response.status_code) 
  
  return zendesk_api_response.json()


def create_csv_file(users, file_path):
  csv_header = list(users[0].keys())
  #csv_body   = list(map(lambda user:user.values(), users))

  with open(file_path, "w") as csv_file:
    csv_writer = csv.DictWriter(csv_file, fieldnames=csv_header)
    csv_writer.writeheader()
    for user in users:
      csv_writer.writerow(user)


def upload_to_s3(file_path, bucket, key_id='', key_secret=''):
  if key_id or key_secret:
    logging.warning('Use instance IAM Role instead of passing Key ID & Secret')    
  
  client = boto3.client('s3', aws_access_key_id=key_id, aws_secret_access_key=key_secret)
  response = client.upload_file(file_path, bucket, file_path)


def main():
  config = fetch_config('main')
  
  user_list = fetch_zendesk_users(config['zendesk']['endpoint'], config['zendesk']['user'], config['zendesk']['password'])
  
  create_csv_file(user_list['users'], config['csv']['file_path'])
  upload_to_s3(config['csv']['file_path'], config['s3']['bucket'], config['iam']['key_id'], config['iam']['key_secret'])
  
  
if __name__ == '__main__':
  main()







