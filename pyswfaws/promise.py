from dateutil.relativedelta import *

from models import Marker as MarkerModel
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

    __slots__ = ['is_ready', 'exception', '_result']

    def __init__(self):
        self.is_ready = False
        self.exception = None
        self._result = None

    @property
    def result(self):
        if self.exception:
            raise self.exception
        if not self.is_ready:
            raise Exception('Promise is not ready yet!')
        return self._result

    @result.setter
    def result(self, val):
        self._result = val


class DistributedActivityPromise(Promise):
    """
    Represents a promise from an activity task when running in distributed mode.

    This promise has special logic for being able to exit the current thread when it is called upon to return a
    result that it does not yet have.
    """

    _logger = logging.getLogger('pyswfaws.DistributedPromise')
    _activity = None
    _failure_states = ('FAILED_TO_SCHEDULE', 'FAILED', 'TIMED_OUT', 'CANCELED')

    def __init__(self, activity):
        self._activity = activity

    @property
    def result(self):
        if self._activity.state in ('SCHEDULED', 'STARTED'):
            # If the task is still running
            self._logger.debug("Stopping execution due to promises not being fulfilled.")
            thread.exit()
        if self._activity.state in self._failure_states:
            # We failed and we don't have retries or it's not a retryable failure
            raise self.exception
        else:
            # We finished
            return self._activity.result

    @property
    def is_ready(self):
        if self._activity.state == 'COMPLETED' or self._activity.state in self._failure_states:
            return True
        return False

    @property
    def exception(self):
        if self._activity.state not in self._failure_states:
            return None
        self._logger.debug('Generating exception')
        return ActivityTaskException(task_name=self._activity.type, task_version=self._activity.version,
                                     task_id=self._activity.id, failure_reason=self._activity.failure_reason,
                                     failure_status=self._activity.state)


class DistributedChildWorkflowPromise(Promise):
    """
    Represents a promise from an child workflow when running in distributed mode.

    This promise has special logic for being able to exit the current thread when it is called upon to return a
    result that it does not yet have.
    """

    _logger = logging.getLogger('pyswfaws.DistributedPromise')
    _cwf = None
    _failure_states = ('FAILED_TO_SCHEDULE', 'FAILED', 'TIMED_OUT', 'CANCELED')

    def __init__(self, cwf):
        self._cwf = cwf

    @property
    def result(self):
        if self._cwf.state in ('SCHEDULED', 'STARTED'):
            # If the task is still running
            self._logger.debug("Stopping execution due to promises not being fulfilled.")
            thread.exit()
        if self._cwf.state in self._failure_states:
            # We failed and we don't have retries or it's not a retryable failure
            raise self.exception
        else:
            # We finished
            return self._cwf.result

    @property
    def is_ready(self):
        if self._cwf.state == 'COMPLETED' or self._cwf.state in self._failure_states:
            return True
        return False

    @property
    def exception(self):
        if self._cwf.state not in self._failure_states:
            return None
        self._logger.debug('Generating exception')
        return ActivityTaskException(task_name=self._cwf.type, task_version=self._cwf.version,
                                     task_id=self._cwf.run_id, failure_reason=self._cwf.failure_reason,
                                     failure_status=self._cwf.state)


class Timer(Promise):
    """
    Defines a Timer in SWF

    This class is a factory that creates times for workflows.  This class is not meant to be instantiated.
    """

    __slots__ = ['is_remote_mode', 'decision_context']

    class SwfTimer(Promise):
        """
        Represents an SWF timer
        """

        def __init__(self, seconds):
            self.is_ready = False

            # Try to get a timer from the timer iterator.  If none exists, make a new timer
            try:
                timer = Timer.decision_context.timers_iter.next()
                self.is_ready = (timer.state == 'COMPLETED')
            except StopIteration:
                Timer.decision_context.decisions.start_timer(str(seconds), Timer.decision_context.get_next_id())

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
            now = datetime.datetime.now()
            self._fire_datetime = now + relativedelta(seconds=+seconds)

        @property
        def is_ready(self):
            if self._fire_datetime <= datetime.datetime.now():
                return True
            return False

        @property
        def result(self):
            while not self.is_ready:
                time.sleep(1)

    @staticmethod
    def get(seconds):
        """
        Gets a timer that will fire at a specified number of seconds into the future
        :param seconds:
        :return:
        """
        if Timer.is_remote_mode:
            return Timer.SwfTimer(seconds)
        else:
            return Timer.LocalTimer(seconds)


class Marker(Promise):
    """
    Defines a Marker in SWF

    This class gets Marker models from the SWF decision context for use in deciders
    """

    __slots__ = ['marker', 'decision_context', 'decision_context', 'is_remote_mode']

    def __init__(self, name, details):
        if not self.is_remote_mode:
            self.marker = MarkerModel(name=name, details=details)
        else:
            # Get the next marker from the dc history
            try:
                self.marker = self.decision_context.user_markers_iter.next()
            except StopIteration:
                # No next marker?  This must be a new marker.
                self.marker = MarkerModel(name=name, details=details)
                self.decision_context.decisions.record_marker(marker_name=name, details=details)


    @property
    def is_ready(self):
        # Markers don't take any time to make, so they're "always ready"
        return True

    @property
    def result(self):
        return self.marker
