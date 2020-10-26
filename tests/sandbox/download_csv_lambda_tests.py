from jobs.download.download_csv_lambda import handler
from jobs.download import config_service

event = {}
event['target'] = 'fenix'

handler(event, None)