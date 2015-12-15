import logging
import traceback
import time
import sys
import uuid

from abc import ABCMeta
from abc import abstractmethod
from collections import OrderedDict

import boto
import venusian
import pyswfaws.decorators

from boto.swf.layer1_decisions import Layer1Decisions
from models import *
from serializers import *
from datastores import *
from promise import Timer as PTimer, Marker as PMarker


class DistributedDecisionWorker:
    """
    This class is the base of all distributed decision workers.

    In order to run a decision task in distributed mode, users must take this class (or a sub class) and instantiate
    it with a decision function and any last-minute configs.  The user should then call `start`, which will begin an
     infinite loop that listens for and acts on activity tasks from SWF.
    """

    logger = logging.getLogger(__name__)

    def __init__(self, decision_function, decision_object=None, swf_domain=None, swf_task_list=None, workflow_type=None,
                 workflow_version=None, aws_access_key_id=None, aws_secret_access_key=None):
        """

        :param decision_function:
        :param swf_domain:
        :param swf_task_list:
        :param workflow_type:
        :param workflow_version:
        :param aws_access_key_id:
        :param aws_secret_access_key:
        :return:
        """

        self._decision_object = decision_object
        self._swf = boto.connect_swf(aws_access_key_id=aws_access_key_id,
                                     aws_secret_access_key=aws_secret_access_key)
        self._decision_function = decision_function

        registry = Registry()
        self._scanner = venusian.Scanner(registry=registry, mode='remote', caller='decision_worker')
        self._scanner.parent_decision_worker = decision_function

        # Some trickery here -- scan the module that the activity worker method is found in
        self._scanner.scan(sys.modules[decision_function.orig.__module__], categories=('pyswfaws.activity_task',
                                                                                       'pyswfaws.decision_task'))

        # More trickery -- make sure that timers know that we're in a remote mode
        PTimer.is_remote_mode = True
        PMarker.is_remote_mode = True

        if hasattr(self._decision_function, 'swf_options'):

            # Give preference to the values in the constructor
            self._swf_domain = self.choose_first_not_none('An SWF domain must be specified by the activity worker '
                                                          'constructor or the activity function', swf_domain,
                                                          decision_function.swf_options['domain'])
            self._swf_task_list = self.choose_first_not_none('An SWF task list must be specified by the activity '
                                                             'worker constructor or the activity function',
                                                             swf_task_list, decision_function.swf_options['task_list'])
            self._wf_type = self.choose_first_not_none('An SWF activity type must be specified by the activity '
                                                             'worker constructor or the activity function',
                                                             workflow_type, decision_function.swf_options['wf_type'])
            self._wf_version = self.choose_first_not_none('An SWF activity version must be specified by the '
                                                                'activity worker constructor or the activity function',
                                                                workflow_version, decision_function.swf_options[
                                                                    'wf_version'])

            self._input_serializer = decision_function.serialization_options['input_serializer']
            self._input_data_store = decision_function.serialization_options['input_data_store']
            self._result_serializer = decision_function.serialization_options['result_serializer']
            self._result_data_store = decision_function.serialization_options['result_data_store']
        else:
            raise Exception('Activity function has no "swf_options" attribute; are you sure the function was properly '
                            'decorated?')

    def handle_no_op(self, decision_task):
        """
        Optional method for intercepting polls that did not receive a task

        This only gets called when the decisioner is in distributed mode.
        :return:
        """
        pass

    def handle_exception(self, exception, decision_context):
        """
        Handles exceptions from the event loop.

        The default behavior is to log it, fail the workflow, and continue.  This method only gets used when in
        distributed mode.
        :param exception:
        :return: True if we want to exit, False otherwise
        """
        self.logger.exception('Exception caught while running the event loop.')
        # Reset the decisions that we want to make; we can't schedule new activities and fail a workflow in the same
        # call
        decision_context.decisions = Layer1Decisions()
        decision_context.decisions.fail_workflow_execution(reason='Decider exception', details=exception.message[:3000])
        return False

    def start(self):
        """
        Starts the event loop

        This method blocks and runs infinitely.  Call this method to start a decisioner in distributed mode after
        construction.
        :return:
        """

        while True:
            self.logger.debug('Polling')

            try:
                decision_task = self._swf.poll_for_decision_task(domain=self._swf_domain, task_list=self._swf_task_list)

                # No-op if we got no events
                if 'events' not in decision_task:
                    self.logger.debug('Calling the no-op handler')
                    self.handle_no_op(decision_task)
                    continue

                self.logger.debug('Received an decision task')

                # Get full event history
                history = decision_task['events']
                next_page_token = decision_task.get('nextPageToken')
                while next_page_token:
                    self.logger.debug('Polling for additional history...')
                    additional_history = self._swf.poll_for_decision_task(domain=self._swf_domain,
                                                                          task_list=self._swf_task_list,
                                                                          next_page_token=next_page_token)
                    if 'events' in additional_history:
                        history.extend(additional_history['events'])

                    decision_task['events'] = list()
                    next_page_token = additional_history.get('nextPageToken')
            except Exception as e:
                self.logger.exception('Exception while polling for a task and/or history')
                raise e

            # Populate the context
            try:
                decision_context = self._populate_decision_context(decision_task, history)

                # Here's where we make the context available to the decorated function
                self._scanner.decision_context = decision_context

                # Make sure that timers get them, too
                PTimer.decision_context = decision_context
                PMarker.decision_context = decision_context
                # And markers

            except Exception as e:
                self.logger.exception('Exception while parsing a workflow history')
                message = str(e)
                if message:
                    message = message[:3000]
                decision_context.decisions = Layer1Decisions()
                decision_context.decisions.fail_workflow_execution(reason='Failed to parse workflow history',
                                                                     details=message)
                self._swf.respond_decision_task_completed(decision_task['taskToken'],
                                                              decision_context.decisions._data)
                return

            # get the args and run the handle function in a thread
            args = (list(), dict())
            if decision_context.workflow.input:
                self.logger.debug('Unpacking input arguments')
                serialized_args = self._input_data_store.get(decision_context.workflow.input)
                args = self._input_serializer.deserialize_input(serialized_args)

            # Do we have a self that we need to pass in?
            if self._decision_object:
                args[0].insert(0, self._decision_object)

            try:
                finished = False
                result = self._decision_function(*args[0], **args[1])
                finished = True
            except SystemExit:
                finished = False
            except Exception as e:
                self.logger.debug('Calling the exception handler')
                should_exit = self.handle_exception(e, decision_context)
                if should_exit:
                    self.logger.debug('Exiting due to return value from handle_exception()')
                    return

            # Is our workflow finished?
            if finished is True:
                swf_result = None
                if result:
                    self.logger.debug('Packing workflow results')
                    serialized_result = self._result_serializer.serialize_result(result)
                    key = '{}-result'.format(decision_context.workflow.run_id)
                    swf_result = self._result_data_store.put(serialized_result, key)
                decision_context.decisions.complete_workflow_execution(result=swf_result)

            self.logger.debug('Returning decisions to SWF.')
            try:
                self._swf.respond_decision_task_completed(decision_task['taskToken'],
                                                          decision_context.decisions._data)
            except Exception:
                self.logger.exception('Error when responding with decision tasks')

    @staticmethod
    def choose_first_not_none(exception_message, *args):
        for arg in args:
            if arg is not None:
                return arg
        raise Exception(exception_message)

    @staticmethod
    def _populate_decision_context(decision_task, history):
        """
        Populates the SwfDecisionContext using a decision task and it's history.
        :param decision_task:
        :param history:
        :return:
        """

        activities = OrderedDict()
        signals = list()
        timers = OrderedDict()
        child_workflows = OrderedDict()
        workflow = Workflow()
        cache_markers = list()
        user_markers = list()
        id_count = 1

        workflow.run_id = decision_task['workflowExecution']['runId']

        for e in history:
            et = e['eventType']

            # Workflow related events
            if et == 'WorkflowExecutionStarted':
                workflow.continued_execution_run_id = e['workflowExecutionStartedEventAttributes'].get('continuedExecutionRunId')
                workflow.input = e['workflowExecutionStartedEventAttributes'].get('input')
                workflow.child_policy = e['workflowExecutionStartedEventAttributes'].get('childPolicty')
                workflow.continued_execution_run_id = e['workflowExecutionStartedEventAttributes'].get('continuedExecutionRunId')
                workflow.execution_start_to_close_timeout = e['workflowExecutionStartedEventAttributes'].get(
                    'executionStartToCloseTimeout')
                workflow.lambda_role = e['workflowExecutionStartedEventAttributes'].get('lambdaRole')
                wf_type = e['workflowExecutionStartedEventAttributes'].get('parentWorkflowExecution')
                if wf_type:
                    workflow.parent_workflow_id = e['workflowExecutionStartedEventAttributes']['parentWorkflowExecution']['workflowId']
                    workflow.parent_run_id = e['workflowExecutionStartedEventAttributes']['parentWorkflowExecution']['runId']
                workflow.tag_list = e['workflowExecutionStartedEventAttributes'].get('tagList')
                workflow.task_list = e['workflowExecutionStartedEventAttributes'].get('taskList')
                workflow.task_priority = e['workflowExecutionStartedEventAttributes'].get('taskPriority')
                workflow.type = e['workflowExecutionStartedEventAttributes']['workflowType']['name']
                workflow.version = e['workflowExecutionStartedEventAttributes']['workflowType']['version']
                workflow.state = 'RUNNING'
            elif et == 'WorkflowExecutionTerminated':
                workflow.state = 'TERMINATED'
                workflow.termination_cause = e['workflowExecutionTerminatedEventAttributes'].get('cause')
                workflow.termination_detail = e['workflowExecutionTerminatedEventAttributes'].get('detail')
                workflow.termination_reason = e['workflowExecutionTerminatedEventAttributes'].get('reason')
            elif et == 'WorkflowExecutionCompleted':
                workflow.state = 'COMPLETED'
                workflow.result = e['workflowExecutionCompletedEventAttributes'].get('result')
            elif et == 'WorkflowExecutionFailed':
                workflow.state = 'FAILED'
                workflow.failure_details = e['workflowExecutionFailedEventAttributes'].get('details')
                workflow.failure_reason = e['workflowExecutionFailedEventAttributes'].get('reason')
            elif et == 'WorkflowExecutionTimedOut':
                workflow.state == 'TIMED_OUT'
            elif et == 'WorkflowExecutionCancelRequested':
                workflow.state = 'CANCEL_REQUESTED'
                workflow.cancellation_cause = e['workflowExecutionCancelRequestedEventAttributes'].get('cause')
            elif et == 'WorkflowExecutionCanceled':
                workflow.state == 'CANCELED'
                workflow.cancellation_details = e['workflowExecutionCanceledEventAttributes'].get('details')

            # Activity task related events
            elif et == 'ActivityTaskScheduled':
                activity_task = Activity()
                id_count += 1
                activity_task.id = e['activityTaskScheduledEventAttributes']['activityId']
                activity_task.type = e['activityTaskScheduledEventAttributes']['activityType']['name']
                activity_task.version = e['activityTaskScheduledEventAttributes']['activityType']['version']
                activity_task.control = e['activityTaskScheduledEventAttributes'].get('control')
                activity_task.input = e['activityTaskScheduledEventAttributes'].get('input')
                activity_task.task_list = e['activityTaskScheduledEventAttributes']['taskList'].get('name')
                activity_task.task_priority = e['activityTaskScheduledEventAttributes'].get('taskPriority')
                activity_task.state = 'SCHEDULED'
                activities[activity_task.id] = activity_task
            elif et == 'ScheduleActivityTaskFailed':
                id = e['scheduleActivityTaskFailed']['activityId']
                activity_task = activities[id]
                activity_task.state = 'FAILED_TO_SCHEDULE'
                activity_task.schedule_failure_cause = e['scheduleActivityTaskFailedEventAttributes'].get('cause')
            elif et == 'ActivityTaskStarted':
                started_id = e['activityTaskStartedEventAttributes']['scheduledEventId']
                activity_id = history[started_id - 1]['activityTaskScheduledEventAttributes']['activityId']
                activities[activity_id].state = 'STARTED'
            elif et == 'ActivityTaskCompleted':
                started_id = e['activityTaskCompletedEventAttributes']['scheduledEventId']
                activity_id = history[started_id - 1]['activityTaskScheduledEventAttributes']['activityId']
                activities[activity_id].state = 'COMPLETED'
                activities[activity_id].result = e['activityTaskCompletedEventAttributes'].get('result')
            elif et == 'ActivityTaskFailed':
                started_id = e['activityTaskFailedEventAttributes']['scheduledEventId']
                activity_id = history[started_id - 1]['activityTaskScheduledEventAttributes']['activityId']
                activities[activity_id].state = 'FAILED'
                activities[activity_id].failure_reason = e['activityTaskFailedEventAttributes'].get('reason')
                activities[activity_id].failure_details = e['activityTaskFailedEventAttributes'].get('details')
            elif et == 'ActivityTaskTimedOut':
                started_id = e['activityTaskTimedOutEventAttributes']['scheduledEventId']
                activity_id = history[started_id - 1]['activityTaskScheduledEventAttributes']['activityId']
                activities[activity_id].state = 'TIMED_OUT'
                activities[activity_id].time_out_details = e['activityTaskTimedOutEventAttributes'].get('details')
                activities[activity_id].time_out_type = e['activityTaskTimedOutEventAttributes'].get(
                    'timeoutType')
            elif et == 'ActivityTaskCanceled':
                started_id = e['activityTaskCanceledEventAttributes']['scheduledEventId']
                activity_id = history[started_id - 1]['activityTaskScheduledEventAttributes']['activityId']
                activities[activity_id].state = 'CANCELED'
                activities[activity_id].cancellation_details = e['activityTaskCanceledEventAttributes']['details']
            elif et == 'ActivityTaskCancelRequested':
                pass
            elif et == 'RequestCancelActivityTaskFailed':
                pass

            # signal related events
            elif et == 'WorkflowExecutionSignaled':
                signal = Signal()
                signal.input = e['workflowExecutionSignaledEventAttributes'].get('input')
                signal.name = e['workflowExecutionSignaledEventAttributes'].get('signalName')
                signals.append(signal)

            # timer related events
            elif et == 'TimerStarted':
                timer = Timer()
                id_count += 1
                timer.id = e['timerStartedEventAttributes']['timerId']
                timer.control = e['timerStartedEventAttributes'].get('control')
                timer.state = 'STARTED'
                timers[timer.id] = timer
            elif et == 'StartTimerFailed':
                id = e['startTimerFailedEventAttributes']['timerId']
                timers[id].state = 'FAILED'
                timers[id].failure_cause = ['startTimerFailedEventAttributes']['cause']
            elif et == 'TimerFired':
                id = e['timerFiredEventAttributes']['timerId']
                timers[id].state = 'COMPLETED'
            elif et == 'TimerCanceled':
                id = e['timerCanceledEventAttributes']['timerId']
                timers[id].state = 'CANCELED'
            elif et == 'CancelTimerFailed':
                pass

            # child workflow stuffs
            elif et == 'ChildWorkflowExecutionCanceled':
                id = e['childWorkflowExecutionCanceledEventAttributes']['workflowExecution']['workflowId']
                child_workflow = child_workflows[id]
                child_workflow.state = 'CANCELED'
            elif et == 'ChildWorkflowExecutionCompleted':
                id = e['childWorkflowExecutionCompletedEventAttributes']['workflowExecution']['workflowId']
                child_workflow = child_workflows[id]
                child_workflow.state = 'COMPLETED'
                child_workflow.result = e['childWorkflowExecutionCompletedEventAttributes']['result']
            elif et == 'ChildWorkflowExecutionFailed':
                id = e['childWorkflowExecutionFailedEventAttributes']['workflowExecution']['workflowId']
                child_workflow = child_workflows[id]
                child_workflow.state = 'FAILED'
                child_workflow.failure_details = e['childWorkflowExecutionFailedEventAttributes']['details']
                child_workflow.failure_reason = e['childWorkflowExecutionFailedEventAttributes']['reason']
            elif et == 'ChildWorkflowExecutionStarted':
                id = e['childWorkflowExecutionStartedEventAttributes']['workflowExecution']['workflowId']
                child_workflow = child_workflows[id]
                child_workflow.continued_execution_run_id = e['childWorkflowExecutionStartedEventAttributes'].get(
                    'continuedExecutionRunId')
                wf_type = e['childWorkflowExecutionStartedEventAttributes'].get('parentWorkflowExecution')
                if wf_type:
                    child_workflow.parent_workflow_id = e['childWorkflowExecutionStartedEventAttributes']['parentWorkflowExecution']['workflowId']
                    child_workflow.parent_run_id = e['childWorkflowExecutionStartedEventAttributes']['parentWorkflowExecution']['runId']
                child_workflow.run_id = e['childWorkflowExecutionStartedEventAttributes']['workflowExecution']['runId']
                child_workflow.state = 'STARTED'
            elif et == 'ChildWorkflowExecutionTerminated':
                id = e['childWorkflowExecutionTerminatedEventAttributes']['workflowExecution']['workflowId']
                child_workflow = child_workflows[id]
                child_workflow.state = 'TERMINATED'
            elif et == 'ChildWorkflowExecutionTimedOutEvent':
                id = e['childWorkflowExecutionTerminatedEventAttributes']['workflowExecution']['workflowId']
                child_workflow = child_workflows[id]
                child_workflow.state = 'TIMED_OUT'
            elif et == 'StartChildWorkflowExecutionFailed':
                id = e['startChildWorkflowExecutionFailedEventAttributes']['workflowExecution']['workflowId']
                child_workflow = child_workflows[id]
                child_workflow.failure_cause = e['startChildWorkflowExecutionFailedEventAttributes']['cause']
            elif et == 'StartChildWorkflowExecutionInitiated':
                child_workflow = Workflow()
                id = e['startChildWorkflowExecutionInitiatedEventAttributes']['workflowId']
                child_workflows[id] = child_workflow
                child_workflow.workflow_id = id
                child_workflow.child_policy = e['startChildWorkflowExecutionInitiatedEventAttributes'].get('childPolicty')
                child_workflow.control = e['startChildWorkflowExecutionInitiatedEventAttributes'].get('control')
                child_workflow.execution_start_to_close_timeout = e['startChildWorkflowExecutionInitiatedEventAttributes'].get(
                    'executionStartToCloseTimeout')
                child_workflow.input = e['startChildWorkflowExecutionInitiatedEventAttributes'].get('input')
                child_workflow.lambda_role = e['startChildWorkflowExecutionInitiatedEventAttributes'].get('lambdaRole')
                child_workflow.tag_list = e['startChildWorkflowExecutionInitiatedEventAttributes'].get('tagList')
                child_workflow.task_list = e['startChildWorkflowExecutionInitiatedEventAttributes'].get('taskList')
                child_workflow.task_priority = e['startChildWorkflowExecutionInitiatedEventAttributes'].get('taskPriority')
                child_workflow.type = e['startChildWorkflowExecutionInitiatedEventAttributes']['workflowType']['name']
                child_workflow.version = e['startChildWorkflowExecutionInitiatedEventAttributes']['workflowType']['version']
                child_workflow.state = 'SCHEDULED'

            elif et == 'MarkerRecorded':
                marker_name = e['markerRecordedEventAttributes']['markerName']
                marker_details = e['markerRecordedEventAttributes']['details']
                marker = Marker(name=marker_name, details=marker_details)

                # Cache markers
                if marker_name == 'cache':
                    cache_markers.append(marker)
                # User markers
                else:
                    user_markers.append(marker)

        swf_decision_context = SwfDecisionContext()

        # Handle task retry statuses
        activities_with_retries = OrderedDict(activities)
        json_serializer = JsonSerializer()
        ids_to_delete = list()
        for activity in activities_with_retries.itervalues():
            if activity.control:
                control_data = json_serializer.deserialize_result(activity.control)
                original_task_id = control_data['original_attempt_task_id']
                if activity.id != original_task_id:
                    # If we have a task that is a retry of another task, replace it in the map, maintaining
                    # the original attempt's key in order to preserve order.
                    # (keys will get dumped, anyway)
                    activities_with_retries[original_task_id] = activity
                    ids_to_delete.append(activity.id)

        for id_to_delete in ids_to_delete:
            # Remove the ids that we moved to elsewhere in the map
            del activities_with_retries[id_to_delete]

        # Put this stuff into the context
        swf_decision_context.activities = activities
        swf_decision_context.activities_iter = iter(activities_with_retries.itervalues())
        swf_decision_context.child_workflows = child_workflows
        swf_decision_context.child_workflows_iter = iter(child_workflows.itervalues())
        swf_decision_context.decision_task = decision_task
        swf_decision_context.signals = signals
        swf_decision_context.signals_iter = iter(signals)
        swf_decision_context.timers = timers
        swf_decision_context.timers_iter = iter(timers.itervalues())
        swf_decision_context.swf_history = history
        swf_decision_context.cache_markers = cache_markers
        swf_decision_context.cache_markers_iter = iter(cache_markers)
        swf_decision_context.user_markers = user_markers
        swf_decision_context.user_markers_iter = iter(user_markers)

        swf_decision_context.workflow = workflow
        swf_decision_context._id_generator = id_count

        return swf_decision_context


class LocalDecisionWorker(object):
    """
    This class is the base of all locally run decision workers.
    """

    logger = logging.getLogger(__name__)

    def __init__(self, decision_function, decision_object=None):
        self._decision_object = decision_object
        self._decision_function = decision_function

        registry = Registry()
        self._scanner = venusian.Scanner(registry=registry, mode='local', caller='decision_worker')
        self._scanner.parent_decision_worker = decision_function

        # Some trickery here -- scan the module that the activity worker method is found in
        self._scanner.scan(sys.modules[decision_function.orig.__module__], categories=('pyswfaws.activity_task',
                                                                                       'pyswfaws.decision_task'))

        # More trickery -- make sure that timers know that we're in a remote mode
        PTimer.is_remote_mode = False
        PMarker.is_remote_mode = False

    def start(self, *args, **kwargs):
        # Do we have a 'self' to pass in?
        if self._decision_object:
            return self._decision_function(self._decision_object, *args, **kwargs)
        else:
            return self._decision_function(*args, **kwargs)


class Registry(object):

    __slots__ = ['registered']

    def __init__(self):
        self.registered = dict()

    def add(self, orig_func, modified_func):
        self.registered[orig_func] = modified_func