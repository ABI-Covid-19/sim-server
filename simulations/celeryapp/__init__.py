import os
from pathlib import Path

from celery import Celery

transport_dir = os.getenv('CELERY_TRANSPORT_FOLDER', '/tmp/celery_transport_dir')

__data_folders = {
    'data': os.path.join(transport_dir, 'data'),
    'processed': os.path.join(transport_dir, 'processed'),
}

for f in __data_folders.values():
    if not os.path.exists(f):
        os.makedirs(f)

app = Celery(__name__)

app.conf.update({
    'broker_url': 'filesystem://',
    'broker_transport_options': {
        'data_folder_in': __data_folders['data'],
        'data_folder_out': __data_folders['data'],
        'data_folder_processed': __data_folders['processed']
    },
    'result_persistent': False,
    'task_serializer': 'json',
    'result_serializer': 'json',
    'accept_content': ['json']
})
