import json


def process_user(data: dict, _user):
    if 'user' in data:
        _user.put(json.dumps(data['user']))
        data.pop('user')

    for val in data.values():
        if isinstance(val, list):
            for element in val:
                if isinstance(element, dict):
                    process_user(element, _user)


def parse(response, data_holder,logger):
    body = response.text

    if len(body) < 13_000:
        # if the size of data is too small that mean it has no been updated
        # the default empty size seems to be 640
        # so I decide 12434 could be a good value in case of small change due to build tag
        logger.warning(f"EMPTY - {response.url}")
        return

    raw_json = body.split('<script id="__NEXT_DATA__" type="application/json">')[1].split(
        '</script>')[0]
    data = json.loads(raw_json)['props']['initialState']['question']

    process_user(data, data_holder['users'])
    data_holder['questions'].put(json.dumps(data))

    logger.info(f"SUCCESS - {response.url}")
    return
