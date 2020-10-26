import json
import boto3
import os

_config = {}
def init():
    global _config
    
    with open('jobs/download/config.json') as c:
        _config = json.load(c)

    environment = os.getenv('ENVIRONMENT')
    _config['env'] = environment if environment is not None else 'DEV'
    
    _config['s3'] = {
        'client': boto3.client('s3'),
        'bucket': os.getenv('S3_BUCKET'),
        'tmp_path': 'jobs/tmp'
    }
    
    _config['local'] = {
        'path': 'tests/files'
    }
     
         
def get_config():
    global _config   
    return _config


def get_env():
    global _config
    return _config['env']


def print_config():
    global _config
    
    for k in _config:
        print(f'{k}: {_config[k]}')
     
        
def get_site_config(target):
    sites = _config['sites']
    
    return next(x for x in sites if x['target'] == target)