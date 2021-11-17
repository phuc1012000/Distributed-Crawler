import time
from concurrent import futures
import boto3
from src.data_holder import get_data_storage, upload_file
from src.logger import create_logger
from src.spider import process
import os

MSG_QUEUE = os.getenv('MSG_QUEUE')
PARALLEL_LEVEL = int(os.getenv('PARALLEL_LEVEL'))
BATCH_MESSAGE = int(os.getenv('BATCH_MESSAGE'))
WAIT_TIMEOUT_MSG = int(os.getenv('WAIT_TIMEOUT_MSG'))


def main():
    _is_working = False
    for message in msg_queue.receive_messages(MessageAttributeNames=['ID'],
                                              MaxNumberOfMessages=BATCH_MESSAGE,
                                              WaitTimeSeconds=WAIT_TIMEOUT_MSG):
        _is_working = True
        begin = time.time()
        file_name = message.message_attributes.get('ID').get('StringValue')
        try:
            logger.info(f'START - {file_name}')
            urls = [url.strip() for url in message.body.split('\n')]

            with futures.ThreadPoolExecutor(max_workers=PARALLEL_LEVEL) as executor:
                for url in urls:
                    executor.submit(process, url, data_holder, logger, msg_queue)

            upload_file(data_holder, file_name, logger)

        except Exception as err:
            logger.critical(err)
        else:
            message.delete()
            logger.info('SUCCESS - FINISH')
        finally:
            logger.info(f'DURATION - {time.time() - begin}')

    return _is_working


if __name__ == '__main__':
    logger = create_logger()
    data_holder = get_data_storage()

    sqs = boto3.resource('sqs')
    msg_queue = sqs.get_queue_by_name(QueueName=MSG_QUEUE)

    while True:
        is_working = main()
        if not is_working:
            logger.info('FINISH - NO MORE TASKS')
            break
