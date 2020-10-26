import requests
import csv
import requests
from datetime import datetime
from time import time
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from jobs.download import config_service
import os


def handler(event, context):
    config_service.init()
    
    target = event['target']
    site_config = config_service.get_site_config(target)
    url = site_config['url']
    
    if site_config == None:
        raise Exception(f'Missing site config for {target}')
    
    try:
        page = download_page_content(url)
    except Exception as e:
        raise e
    
    csv_page = None
    if target == 'fenix':
        soup = BeautifulSoup(page, 'html.parser')

        gsheet_url = urlparse(soup.body.find('iframe')['src'])
        gsheet_url = gsheet_url._replace(path=gsheet_url.path[:-4], query='output=csv')
        
        try:
            csv_page = download_page_content(gsheet_url.geturl())
        except Exception as e:
            raise e
        
    if csv_page is None:
        raise Exception('Process failed')
    
    now = datetime.utcnow()
    key = f'{int(time())}.csv'
    path = f'{target}/{now.strftime("%Y-%m-%d")}'
        
    if config_service.get_env() == 'PROD':
        save_page_s3(csv_page, path, key)
    else:
        save_page_local(csv_page, path, key)      
            

def save_page_s3(page, path, key):
    s3 = config_service.get_config()['s3']
    s3_client = s3['client']
    s3_bucket = s3['bucket']
    tmp_path = f'{s3["tmp_path"]}/{key}'
    
    with open(tmp_path, 'w+') as f:
        f.write(page)
    
    s3_client.upload_file(tmp_path, s3_bucket, f'{path}/{key}')
    
    os.remove(tmp_path)
    
    
def save_page_local(page, path, key):
    date = datetime.utcnow().strftime('')
    local = config_service.get_config()['local']  
    target = f'{local["path"]}/{path}'

    if os.path.exists(target) == False:
        os.mkdir(target)
        
    with open(f'{target}/{key}', 'w+') as f:
        f.write(page)
    
    
def download_page_content(url):
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.text
    
    raise Exception(f'Can\'t download page {url}')
    
    