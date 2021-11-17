import queue
import os

import boto3

bucket_name = os.getenv('BUCKET_NAME')


def get_data_storage():
    _data = {
        'users': queue.Queue(),
        'questions': queue.Queue(),
    }

    return _data


def upload_file(data, file, _logger, bucket=bucket_name):
    s3 = boto3.client('s3')

    for key, stored_queue in data.items():
        stored = ''
        file_name = f'{file}_{key}'

        _logger.info(f'PREPARING_UPLOAD - {file_name}')
        while not stored_queue.empty():
            stored += stored_queue.get() + '\n'

        s3.put_object(
            Body=stored,
            Bucket=bucket,
            Key=file_name
        )

        _logger.info(f'SUCCESS_UPLOAD - {file_name}')
