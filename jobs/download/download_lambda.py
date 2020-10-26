import boto3
from jobs.download import config_service

client = boto3.client("lambda")

def handler(event, context):
    targets = ["fenix"]
    
    for target in targets:
        site_config = config_service.get_site_config(target)
        
        site_type = site_config['type']
        if site_type == 'csv':
            client.invoke(
                FunctionName="download_csv_lambda",
                InvocationType="Event",
                Payload={
                    "target": target
                    }
            )
            
        elif site_type == 'page':
            client.invoke(
                FunctionName="download_page_lambda",
                InvocationType="Event",
                Payload={
                    "target": target
                    }
            )
            
        else:
            raise Exception(f"Unknown target type {site_type}")
            
        
    