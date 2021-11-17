import json
import requests
import time


def process_user(data: dict, _user):
    if 'user' in data:
        _user.put(json.dumps(data['user']))
        data.pop('user')

    for val in data.values():
        if isinstance(val, list):
            for element in val:
                if isinstance(element, dict):
                    process_user(element, _user)


def process(url, users, question, logger, msg_queue, retry=False):
    try:
        response = requests.get(url, timeout=5)
    except requests.exceptions.Timeout:
        if not retry:
            logger.warning(f'TIMEOUT - {url}')
            time.sleep(3)  # slow down in case of spamming sever
            process(url, users, question, logger, msg_queue, retry=True)
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
            time.sleep(3)
            process(url, users, question, logger, msg_queue, retry=True)
        else:
            logger.error(f"{response.status_code} - {response.url}")
            msg_queue.send_message(MessageBody=url, MessageAttributes={
                'ID': {
                    'StringValue': f'RETRY_{response.status_code} - {url.rsplit("/", 1)[1]}',
                    'DataType': 'String'
                }
            })

        return

    body = response.text

    if len(body) < 13_000:
        # if the size of data is too small that mean it has no been updated
        # the default empty size seems to be 640
        # so I decide 12434 could be a good value in case of small change due to build tag
        logger.warning(f"EMPTY - {response.url}")
        return

    ########################################################
    # parse logic
    raw_json = body.split('<script id="__NEXT_DATA__" type="application/json">')[1].split(
        '</script>')[0]
    data = json.loads(raw_json)['props']['initialState']['question']

    process_user(data, users)

    #########################################################
    question.put(json.dumps(data))
    logger.info(f"SUCCESS - {response.url}")

    return
