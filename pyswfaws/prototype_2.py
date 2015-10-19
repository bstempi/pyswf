from test import TestActivityWorkerA
from models import SwfDecisionContext

import logging


if __name__ == '__main__':
    logging.basicConfig()
    worker = TestActivityWorkerA()
    worker.start()
