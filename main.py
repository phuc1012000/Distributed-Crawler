import queue
import time
from concurrent import futures
import boto3
from src.logger import create_logger
from src.spider import process
import os

bucket_name = os.getenv('BUCKET_NAME')
msg_queue_name = os.getenv('MSG_QUEUE')
parallel_level = os.getenv('PARALLEL_LEVEL')


def upload_file(queue, s3, file_name, logger, bucket=bucket_name):
    stored = ''

    logger.info(f'PREPARING_UPLOAD - {file_name}')
    while not queue.empty():
        stored += queue.get() + '\n'

    s3.put_object(
        Body=stored,
        Bucket=bucket,
        Key=file_name
    )

    logger.info(f'SUCCESS_UPLOAD - {file_name}')


def main():
    is_working = False
    for message in msg_queue.receive_messages(MessageAttributeNames=['ID'],
                                              MaxNumberOfMessages=10,
                                              WaitTimeSeconds=3):
        is_working = True
        begin = time.time()
        file_name = message.message_attributes.get('ID').get('StringValue')
        try:
            logger.info(f'START - {file_name}')
            urls = [url.strip() for url in message.body.split('\n')]

            with futures.ThreadPoolExecutor(max_workers=int(parallel_level)) as executor:
                for url in urls:
                    executor.submit(process, url, user, questions, logger, msg_queue)

            upload_file(questions, s3, f'{file_name}_questions', logger)
            upload_file(user, s3, f'{file_name}_users', logger)

        except Exception as err:
            logger.critical(err)
        else:
            message.delete()
            logger.info('SUCCESS - FINISH')
        finally:
            logger.info(f'DURATION - {time.time() - begin}')

    return is_working


if __name__ == '__main__':
    logger = create_logger()
    user = queue.Queue()
    questions = queue.Queue()

    sqs = boto3.resource('sqs')
    msg_queue = sqs.get_queue_by_name(QueueName=msg_queue_name)

    s3 = boto3.client('s3')

    while True:
        is_working = main()
        if not is_working:
            logger.info('FINISH - NO MORE TASKS')
            break
