import logging

# The commented codes are the code made for a single machine
# before moving to consider msg_queue as a single source truth
# and s3 and the only reliable storage instead of shared volumned
# between distributed running clustered


def create_logger():
    log = logging.getLogger('logger')
    log.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(levelname)s - %(message)s - %(asctime)s')

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    log.addHandler(ch)

    # fh = logging.FileHandler('error', mode='a', encoding='utf-8')
    # fh.setLevel(logging.WARNING)
    # ch.setFormatter(formatter)
    # log.addHandler(fh)

    return log


# def data_queue(data):
#     logger = logging.getLogger(data)
#     logger.setLevel(logging.INFO)
#     ch = logging.FileHandler(data, mode='a', encoding='utf-8')
#     ch.setFormatter(logging.Formatter('%(message)s'))
#     logger.addHandler(ch)
#     return logger
#
#
# def questions_queue(subjects: list):
#     return [data_queue(subject) for subject in subjects]
