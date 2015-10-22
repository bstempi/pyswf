from dateutil import relativedelta

from models import SwfDecisionContext
from serializers import JsonSerializer
from pyswfaws.exceptions import ActivityTaskException

import thread
import datetime
import pytz
import time
import logging


class Promise(object):
    """
    Base for all promises.

    For an explanation of a promise, see https://en.wikipedia.org/wiki/Futures_and_promises
    """
    result = None
    is_ready = False


class DistributedPromise(Promise):
    """
    Represents a promise when running in distributed mode.

    This promise has special logic for being able to exit the current thread when it is called upon to return a
    result that it does not yet have.
    """

    _logger = logging.getLogger('pyswfaws.DistributedPromise')
    _activity = None
    _attempts = 1
    _failure_states = ('FAILED_TO_SCHEDULE', 'FAILED', 'TIMED_OUT', 'CANCELED')
    _serializer = JsonSerializer()
    _original_attempt_task_id = None
    _result = None

    def __init__(self, attempts, retry_states):
        self._max_attempts = attempts
        self._retry_states = retry_states

    @property
    def result(self):
        if self._activity.state in ('SCHEDULED', 'STARTED'):
            # If the task is still running
            self._logger.debug("Stopping execution due to promises not being fulfilled.")
            thread.exit()
        if self._activity.state in self._failure_states and self._attempts < self._max_attempts:
            # If we are in a retryable state and we have retries left
            self._logger.debug("Stopping execution due to activity task requiring a retry.")
            control_data = dict()
            control_data['attempts'] = self._attempts + 1
            control_data['original_attempt_task_id'] = self._original_attempt_task_id
            SwfDecisionContext.decisions.schedule_activity_task(activity_id=SwfDecisionContext.get_next_id(),
                                                                activity_type_name=self._activity.type,
                                                                activity_type_version=self._activity.version,
                                                                task_list=self._activity.task_list,
                                                                input=self._activity.input,
                                                                control=self._serializer.serialize(control_data))
            thread.exit()
        if self._activity.state in self._failure_states:
            # We failed and we don't have retries or it's not a retryable failure
            raise ActivityTaskException(task_name=self._activity.type, task_version=self._activity.version,
                                        task_id=self._activity.id, failure_reason=self._activity.failure_reason,
                                        failure_status=self._activity.state)
            thread.exit()
        else:
            # We finished
            return self._result

    @property
    def is_ready(self):
        if self._activity.state == 'COMPLETED':
            return True
        return False

    def set_activity(self, activity):
        self._activity = activity
        if activity.control is not None:
            control_data = self._serializer.deserialize(activity.control)
            self._attempts = control_data['attempts']
            self._original_attempt_task_id = control_data['original_attempt_task_id']
        else:
            self._attempts = 1
            self._original_attempt_task_id = activity.id

    def set_result(self, result):
        self._result = result


class Timer(Promise):
    """
    Defines a Timer in SWF
    """

    class SwfTimer(Promise):
        """
        Represents an SWF timer
        """

        def __init__(self, seconds):

            # Try to get a timer from the timer iterator.  If none exists, make a new timer
            try:
                timer = SwfDecisionContext.timers_iter.next()
                self.is_ready = timer.state == 'COMPLETED'
            except StopIteration:
                SwfDecisionContext.decisions.start_timer(seconds, SwfDecisionContext.get_next_id())

        @property
        def result(self):
            if self.is_ready is not True:
                # The user wants to cash in on this promise, but it's not ready.  Bail.
                thread.exit()


    class LocalTimer(Promise):
        """
        Represents a timer that will run locally
        """

        def __init__(self, seconds):
            now = datetime.datetime.now(pytz.utc)
            self._fire_datetime = now + relativedelta(seconds=+seconds)

        @property
        def is_ready(self):
            if self._fire_datetime >= datetime.datetime.now(pytz.utc):
                return True
            return False

        @property
        def result(self):
            while not self.is_ready():
                time.sleep(1)

    @staticmethod
    def get(seconds):
        """
        Gets a timer that will fire at a specified number of seconds into the future
        :param seconds:
        :return:
        """
        mode = SwfDecisionContext.mode
        if mode == SwfDecisionContext.Distributed:
            return Timer.SwfTimer(seconds)
        elif mode == SwfDecisionContext.SerialLocal:
            return Timer.LocalTimer(seconds)
        else:
            raise Exception('SwfDecisionContext mode {} not recognized'.format(mode))
