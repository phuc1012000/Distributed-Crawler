import requests
import time
from src.parser import parse
import os

REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT'))
SLEEP_RETRY = int(os.getenv('SLEEP_RETRY'))


def process(url, data_holder, logger, msg_queue, retry=False):
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
    except requests.exceptions.Timeout:
        if not retry:
            logger.warning(f'TIMEOUT - {url}')
            time.sleep(SLEEP_RETRY)  # slow down in case of spamming sever
            process(url, data_holder, logger, msg_queue, retry=True)
        else:
            logger.error(f"TIMEOUT_RETRY - {url}")
            msg_queue.send_message(MessageBody=url, MessageAttributes={
                'ID': {
                    'StringValue': f'RETRY_TIMEOUT - {url.rsplit("/", 1)[1]}',
                    'DataType': 'String'
                }
            })

        return

    if response.status_code != 200:

        logger.warning(f"RETRY - {response.url}")
        if not retry:
            time.sleep(SLEEP_RETRY)
            process(url, data_holder, logger, msg_queue, retry=True)
        else:
            logger.error(f"{response.status_code} - {response.url}")
            msg_queue.send_message(MessageBody=url, MessageAttributes={
                'ID': {
                    'StringValue': f'RETRY_{response.status_code} - {url.rsplit("/", 1)[1]}',
                    'DataType': 'String'
                }
            })
        return

    try:
        parse(response, data_holder, logger)
    except Exception as err:
        # custom pass fail logic
        raise err
