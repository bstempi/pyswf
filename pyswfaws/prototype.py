from test import TestDeciderWithCachedResult
from models import SwfDecisionContext

import logging


if __name__ == '__main__':
    logging.basicConfig()
    decider = TestDeciderWithCachedResult(mode=SwfDecisionContext.Distributed)
    decider.start()
