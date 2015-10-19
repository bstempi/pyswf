from abc import ABCMeta, abstractmethod

import logging
import uuid

from boto.swf.layer1_decisions import Layer1Decisions
from models import SwfDecisionContext, Activity
from promise import *

import boto



class ActivityWorker:
    """
    This class is the base of all activity workers.  It is responsible for the event loop, message serialization, and
    message storage.  Users are expected to extend this class and to provide an implementation of handle_task to carry
    out their work.
    """

    __metaclass__ = ABCMeta
    logger = logging.getLogger('pyswfaws.ActivityWorker')

    class Meta:
        """
        All instances of an ActivityWorker will need a Meta class in order to define certain behavior.  This is modeled
        after Django.
        """
        pass

    def __init__(self, swf_domain=None, swf_task_list=None, activity_type=None,
                 activity_version=None, aws_access_key_id=None, aws_secret_access_key=None):
        """
        Inits an activity worker
        :param swf_domain: SWF domain used by this worker
        :param swf_task_list: SWF task list that this worker is listening to
        :param aws_access_key_id: Access key to use for S3 and SWF.  If none is supplied, boto will fallback to looking for credentials elsewhere.
        :param aws_secret_access_key: Secret key to use for S3 and SWF.  If none is supplied, boto will fallback to looking for credentials elsewhere.
        :return:
        """
        self._swf = boto.connect_swf(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

        # Give preference to the values in the constructor
        self._swf_domain = swf_domain
        self._swf_task_list = swf_task_list
        self._activity_type = activity_type
        self._activity_version = activity_version

        if not hasattr(self, 'Meta'):
            raise Exception('Every activity worker class must have an inner Meta class to provide configurations')

        # Default to values in the Meta class
        if self._swf_domain is None:
            self._swf_domain = getattr(self.Meta, 'swf_domain')
        if self._swf_task_list is None:
            self._swf_task_list = getattr(self.Meta, 'swf_task_list')
        if self._activity_type is None:
            self._activity_type = getattr(self.Meta, 'activity_type')
        if self._activity_version is None:
            self._activity_version = getattr(self.Meta, 'activity_version')

        # Make sure these values got set somehow
        if self._swf_domain is None:
            raise Exception('swf_domain must be set in either the constructor or the Meta class.')
        if self._swf_task_list is None:
            raise Exception('swf_task_list must be set in either the constructor or the Meta class.')
        if self._activity_type is None:
            raise Exception('activity_version must be set in either the constructor or the Meta class.')
        if self._activity_version is None:
            raise Exception('activity_version must be set in either the constructor or the Meta class.')

    @abstractmethod
    def handle_task(self, **kwargs):
        """
        Method to handle activity tasks that come from SWF.

        There are some special behaviors here:

        * Calls that don't throw exceptions automatically cause an ActivityTaskCompletedEvent to be sent to SWF.
        * If the call returns data, it will be returned using the serializer and data store via the result field.
        * Exceptions thrown by this method are caught and passed to handle_exception.  This is designed to handle unexpected exceptions.

        :param activity_task: The activity task that came from SWF
        :param parsed_message: A parsed message, if one exists
        :param input: The input from the activity task, if one exists
        :return: any return value will be serlialized and delivered via the designated serializer and data store
        declared in the Meta class, respectively.  If none are declared, the return value will be discarded.
        """
        raise Exception("Not implemented")

    def handle_no_op(self, activity_task):
        """
        Optional method for intercepting polls that did not receive a task
        :return:
        """
        pass

    def handle_exception(self, exception, activity_task):
        """
        Handles exceptions from the event loop.

        The default behavior is to log it, back-off, and continue.
        :param exception:
        :param activity_task:
        :return: True if we want to exit, False otherwise
        """
        self.logger.exception('Exception caught while running the event loop.')
        self._swf.respond_activity_task_failed(task_token=activity_task['taskToken'],
                                               reason='Activity exception', details=exception.message[:30000])
        return False

    def start(self):
        """
        Starts the event loop.  This method blocks and runs infinitely.
        :return:
        """
        while True:
            try:
                self.logger.debug('Polling')
                activity_task = self._swf.poll_for_activity_task(domain=self._swf_domain, task_list=self._swf_task_list)

                # No-op if we got no events
                if 'activityId' not in activity_task:
                    self.logger.debug('Calling the no-op handler')
                    self.handle_no_op(activity_task)
                    continue

                if 'input' in activity_task and activity_task['input'] != '':
                    parsed_message = self._unpack_input(activity_task)
                else:
                    parsed_message = None

                # Let the user handle it
                self.logger.debug("Calling the user's handler")

                # In this case, result will be a promise.  Be sure to treat it as such.
                if parsed_message is None:
                    parsed_message = list(list(), dict())
                result = self.handle_task(*parsed_message[0], **parsed_message[1])

                swf_result = self._pack_result(activity_task, result.result)
                self._swf.respond_activity_task_completed(activity_task['taskToken'], swf_result)

            except Exception as e:
                self.logger.debug('Calling the exception handler')
                should_exit = self.handle_exception(e, activity_task)
                if should_exit:
                    self.logger.debug('Exiting due to return value from handle_exception()')
                    return

    def _unpack_input(self, activity_task):
        """
        Unpacks the message using the DataStore/Serializer declared in the Meta class.

        Override if the DataStore/Serializer paradigm doesn't work for you.
        :return: A serialized message if there is a data store but no serializer, a deserialized message if both exist,
        and None if there is no data store.
        """

        if 'input' not in activity_task:
            return None

        serialized_message = self.Meta.data_store.get(activity_task['input'])

        # If there is a data store defined, but no serializer, assume the "Null Serializer."  Just return the data
        # without further processing.
        self.logger.debug('Message was unpacked and deserialized')
        return self.Meta.serializer.deserialize(serialized_message)

    def _pack_input(self, input):
        """
        Packs the contents the message using the DataStore/Serializer declared in the Meta class.
        :return: the contents to be placed in the input field
        """
        self.logger.debug('Packing a message')

        serialized_message = self.Meta.serializer.serialize(input)

        return self.Meta.data_store.put(serialized_message, '{}-{}'.format(self._activity_type, uuid.uuid4()))

    def _pack_result(self, activity_task, result):
        """
        Packs the result using the DataStore/Serlializer declared in the Meta class.

        The result of this is packed into the ActivityTaskCompletedEvent's result field.
        Override if the DataStore/Serializer paradigm doesn't work for you.
        :param activity_task:
        :param result: an unserialized result message
        :return: a result suitable for the result field of an ActivityTaskCompletedEvent
        """
        self.logger.debug('Packing an activity task result')
        if not hasattr(self.Meta, 'serializer'):
            self.logger.debug('No serializer attr defined in Meta; not serializing result')
            return None

        serialized_message = self.Meta.serializer.serialize(result)
        return self.Meta.data_store.put(serialized_message,
                                        activity_task['workflowExecution']['runId'])

        self.logger.debug('Message was unpacked and deserialized')
        return Meta.serializer.deserialize(serialized_message)

    def _unpack_result(self, activity_task):
        """
        Unpacks the result using the DataStore/Serlializer declared in the Meta class.

        :param activity_task:
        :return:
        """
        self.logger.debug('Unpacking an activity task result')
        serialized_result = self.Meta.data_store.get(activity_task.result)
        self.logger.debug('Result was unpacked and deserialized')
        return self.Meta.serializer.deserialize(serialized_result)

    @staticmethod
    def activity_task(f, attempts=5, retry_states=('FAILED_TO_SCHEDULE', 'FAILED', 'TIMED_OUT')):
        """
        Used to wrap the handle() method of the activity worker
        :param f:
        :return:
        """

        def decisioner_wrapper(self, *args, **kwargs):
            """
            Wraps an activity task that is being called by a decisioner.

            This mode is used when the activity worker is invoked from a decisioner.  It pays attention to the
            SwfDecisionContext to figure out if the activity worker function should be called directly or if it is being
            run in a distributed manor via SWF.
            :return:
            """

            if SwfDecisionContext.mode == SwfDecisionContext.SerialLocal:
                p = Promise()
                val = f(self, *args, **kwargs)
                p.is_ready = True
                p.result = val
            elif SwfDecisionContext.mode == SwfDecisionContext.Distributed:
                p = DistributedPromise(attempts=attempts, retry_states=retry_states)
                if SwfDecisionContext.activities is None:
                    SwfDecisionContext.activities = list()
                if SwfDecisionContext.activities_iter is None:
                    SwfDecisionContext.activities_iter = iter(SwfDecisionContext.activities)

                # Attempt to iterate through activities
                try:
                    activity = SwfDecisionContext.activities_iter.next()
                    p.set_activity(activity)
                    if activity.result:
                        p.set_result(self._unpack_result(activity))
                    return p
                except StopIteration:

                    if SwfDecisionContext.decisions is None:
                        SwfDecisionContext.decisions = Layer1Decisions()

                    # If we've run out of results, then I suppose that we're hitting new tasks
                    SwfDecisionContext.decisions.schedule_activity_task(activity_id=SwfDecisionContext.get_next_id(),
                                                                        activity_type_name=self._activity_type,
                                                                        activity_type_version=self._activity_version,
                                                                        task_list=self._swf_task_list,
                                                                        input=self._pack_input(input=(args, kwargs)))
                    activity = Activity(state='SCHEDULED')
                    p.set_activity(activity)
            else:
                raise Exception('Parent context is in an unknown mode')
            return p
        return decisioner_wrapper
