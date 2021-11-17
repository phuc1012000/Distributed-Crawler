import logging


def create_logger():
    log = logging.getLogger('logger')
    log.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(levelname)s - %(message)s - %(asctime)s')

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    log.addHandler(ch)

    return log
