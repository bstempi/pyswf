import logging
import traceback
import time
import sys
import uuid

from abc import ABCMeta
from collections import OrderedDict

import boto

from boto.swf.layer1_decisions import Layer1Decisions
from models import *
from serializers import JsonSerializer


class DecisionWorker:
    """
    This class is the base of all decision workers.  It is responsible for the event loop, message serialization, and
    message storage.  It also help manages workflow state.
    """

    __metaclass__ = ABCMeta
    logger = logging.getLogger('pyswfaws.DecisionWorker')

    class Meta:
        """
        All instances of a DecisionWorker will need a Meta class in order to define certain behavior.  This is modeled
        after some of the classes in Django.
        """
        pass

    def __init__(self, mode, swf_domain=None, swf_task_list=None, aws_access_key_id=None, aws_secret_access_key=None):
        """
        Inits a decision worker ready for running
        :param mode: One of the modes in the SwfDecisionContext.
        :param swf_domain: SWF domain used by this worker
        :param swf_task_list: SWF task list that this worker is listening to
        :param aws_access_key_id: Access key to use for S3 and SWF.  If none is supplied, boto will fallback to looking for credentials elsewhere.
        :param aws_secret_access_key: Secret key to use for S3 and SWF.  If none is supplied, boto will fallback to looking for credentials elsewhere.
        :return:
        """

        # Make sure there's a Meta class
        if not hasattr(self, 'Meta'):
            raise Exception('Every decision worker class must have an inner Meta class to provide configurations')

        if mode in (SwfDecisionContext.SerialLocal, ):
            # Nothing else to do here
            pass
        elif mode == SwfDecisionContext.Distributed:
            self.logger.debug('Starting SWF client')

            self._swf = boto.connect_swf(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

            # Give preference to the values in the constructor
            self._swf_domain = swf_domain
            self._swf_task_list = swf_task_list

            # Default to values in the Meta class
            if self._swf_domain is None:
                self._swf_domain = getattr(self.Meta, 'swf_domain')
            if self._swf_task_list is None:
                self._swf_task_list = getattr(self.Meta, 'swf_task_list')

            # Make sure these values got set somehow
            if self._swf_domain is None:
                raise Exception('swf_domain must be set in either the constructor or the Meta class when running in '
                                'distributed mode.')
            if self._swf_task_list is None:
                raise Exception('swf_task_list must be set in either the constructor or the Meta class when running '
                                'in distributed mode.')
        else:
            raise Exception('Mode {} is not valid'.format(mode))

    def handle_no_op(self, decision_task):
        """
        Optional method for intercepting polls that did not receive a task

        This only gets called when the decisioner is in distributed mode.
        :return:
        """
        pass

    def handle_exception(self, exception):
        """
        Handles exceptions from the event loop.

        The default behavior is to log it, fail the workflow, and continue.  This method only gets used when in
        distributed mode.
        :param exception:
        :return: True if we want to exit, False otherwise
        """
        self.logger.exception('Exception caught while running the event loop.')
        SwfDecisionContext.decisions.fail_workflow_execution(reason='Decider exception', details=exception.message[:3000])
        return False

    def handle(self, **kwargs):
        """
        This method handles each decision task

        Call this method directly in order to handle decisions in one of the non-distributed modes.

        NOTE:  It only accepts keyword arguments.  NO POSITIONAL ARGS.
        :param args:
        :param kwargs:
        :return:
        """
        raise NotImplementedError('Handler method must exist')

    def _unpack_message(self, workflow):
        """
        Unpacks the message using the DataStore/Serializer declared in the Meta class.

        Override if the DataStore/Serializer paradigm doesn't work for you.
        :return: A serialized message if there is a data store but no serializer, a deserialized message if both exist,
        and None if there is no data store.
        """

        self.logger.debug('Unpacking a message')
        if workflow.input is None or workflow.input == '':
            return dict()
        serialized_message = self.Meta.data_store.get(workflow.input)
        self.logger.debug('Message was unpacked and deserialized')
        return self.Meta.serializer.deserialize(serialized_message)

    def _pack_result(self, workflow, result):
        """
        Packs the result using the DataStore/Serlializer declared in the Meta class.

        Override if the DataStore/Serializer paradigm doesn't work for you.
        :param workflow:
        :param result: an unserialized result message
        :return: a result suitable for the result field of an ActivityTaskCompletedEvent
        """

        serialized_message = self.Meta.serializer.serialize(result)
        return self.Meta.data_store.put(serialized_message, '{}-result'.format(workflow.run_id))

        self.logger.debug('Message was unpacked and deserialized')
        return Meta.serializer.deserialize(serialized_message)

    def start(self):
        """
        Starts the event loop

        This method blocks and runs infinitely.  Call this method to start a decisioner in distributed mode after
        construction.
        :return:
        """

        while True:
            try:
                SwfDecisionContext.mode = SwfDecisionContext.Distributed
                self.logger.debug('Polling')
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
                        history.append(additional_history['events'])

                    decision_task['events'] = list()
                    next_page_token = additional_history.get('nextPageToken')

                # Populate the context and make sure its' in teh right mode
                self._populate_decision_context(decision_task, history)

                # get the args and run the handle function in a thread
                args = self._unpack_message(SwfDecisionContext.workflow)

                try:
                    result = self.handle(**args)
                    SwfDecisionContext.finished = True
                    SwfDecisionContext.output = result
                except SystemExit:
                    SwfDecisionContext.finished = False

                # Is our workflow finished?
                if SwfDecisionContext.finished is True:
                    if result:
                        # We have output to attach
                        result = self._pack_result(SwfDecisionContext.workflow, result)
                    SwfDecisionContext.decisions.complete_workflow_execution(result=result)
            except Exception as e:
                if SwfDecisionContext.mode == SwfDecisionContext.Distributed:
                    self.logger.debug('Calling the exception handler')
                    should_exit = self.handle_exception(e)
                    if should_exit:
                        self.logger.debug('Exiting due to return value from handle_exception()')
                        return
                else:
                    raise e
            finally:
                self.logger.debug('Returning decisions to SWF.')
                try:
                    self._swf.respond_decision_task_completed(decision_task['taskToken'],
                                                              SwfDecisionContext.decisions._data)
                except Exception:
                    self.logger.exception('Error when responding with decision tasks')

    @staticmethod
    def nondeterministic(f):
        """
        Decorator for wrapping non-deterministic methods so that they aren't called more than once

        This is an alias for @cached
        :param f:
        :return:
        """
        return DecisionWorker.cached(f)

    @staticmethod
    def cached(f):
        """
        Decorator for wrapping methods that we only want to call once and cache

        This is useful for saving the results of an expensive or nondeterministic operation.  Using cached values may
        help speed-up replay.
        :param f:
        :return:
        """

        def wrapper(self, *args, **kwargs):
            if SwfDecisionContext.mode == SwfDecisionContext.Distributed:
                try:
                    cached_result = SwfDecisionContext.cache_markers_iter.next()
                    return self.Meta.serializer.deserialize(cached_result.details)
                except StopIteration:
                    # If we're here, then this guy hasn't been called before.  Call it and cache the output.
                    result = f(self, *args, **kwargs)
                    marker_details = self.Meta.data_store.put(self.Meta.serializer.serialize(result), str(uuid.uuid4()))
                    SwfDecisionContext.decisions.record_marker('cache', marker_details)
                    return result
            else:
                return f(self, *args, **kwargs)

        return wrapper

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
                activity_task.id = e['activityTaskScheduledEventAttributes']['activityId']
                activity_task.type = e['activityTaskScheduledEventAttributes']['activityType']['name']
                activity_task.version = e['activityTaskScheduledEventAttributes']['activityType']['version']
                activity_task.control = e['activityTaskScheduledEventAttributes'].get('control')
                activity_task.input = e['activityTaskScheduledEventAttributes'].get('input')
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
                timer.id = e['timerStartedEventAttributes']['timerId']
                timer.control = e['timerStartedEventAttributes'].get('control')
                timer.state = 'STARTED'
                timer[timer.id] = timer
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
            elif et == 'childWorkflowExecutionCanceledEventAttributes':
                pass
            elif et == 'childWorkflowExecutionCompletedEventAttributes':
                pass
            elif et == 'childWorkflowExecutionFailedEventAttributes':
                pass
            elif et == 'childWorkflowExecutionStartedEventAttributes':
                child_workflow = Workflow()
                child_workflow.continued_execution_run_id = e['workflowExecutionStarted'].get('continuedExecutionRunId')
                child_workflow.input = e['workflowExecutionStarted'].get('input')
                child_workflow.child_policy = e['workflowExecutionStarted'].get('childPolicty')
                child_workflow.continued_execution_run_id = e['workflowExecutionStarted'].get('continuedExecutionRunId')
                child_workflow.execution_start_to_close_timeout = e['workflowExecutionStarted'].get(
                    'executionStartToCloseTimeout')
                child_workflow.lambda_role = e['workflowExecutionStarted'].get('lambdaRole')
                wf_type = e['workflowExecutionStarted'].get('parentWorkflowExecution')
                if wf_type:
                    child_workflow.parent_workflow_id = e['workflowExecutionStarted']['parentWorkflowExecution']['workflowId']
                    child_workflow.parent_run_id = e['workflowExecutionStarted']['parentWorkflowExecution']['runId']
                child_workflow.run_id = e['workflowExecutionStarted']['workflowExecution']['runId']
                child_workflow.tag_list = e['workflowExecutionStarted'].get('tagList')
                child_workflow.task_list_name = e['workflowExecutionStarted'].get('taskList')
                child_workflow.task_priority = e['workflowExecutionStarted'].get('taskPriority')
                child_workflow.type = e['workflowExecutionStarted']['workflowType']['name']
                child_workflow.version = e['workflowExecutionStarted']['workflowType']['version']
                child_workflow.state = 'RUNNING'
                child_workflows[child_workflow.run_id] = child_workflow
            elif et == 'childWorkflowExecutionTerminatedEventAttributes':
                run_id = e['childWorkflowExecutionTerminatedEventAttributes']['workflowExecution']['runId']
                child_workflow = child_workflows[run_id]
                child_workflow.state = 'TERMINATED'
            elif et == 'childWorkflowExecutionTimedOutEventAttributes':
                run_id = e['childWorkflowExecutionTerminatedEventAttributes']['workflowExecution']['runId']
                child_workflow = child_workflows[run_id]
                child_workflow.state = 'TIMEDOUT'
            elif et == 'startChildWorkflowExecutionFailedEventAttributes':
                run_id = e['startChildWorkflowExecutionFailedEventAttributes']['workflowExecution']['runId']
                child_workflow = child_workflows[run_id]
                child_workflow.failure_cause = e['startChildWorkflowExecutionFailedEventAttributes']['cause']
            elif et == 'startChildWorkflowExecutionInitiatedEventAttributes':
                pass

            # Cache markers
            elif et == 'MarkerRecorded':
                marker_name = e['markerRecordedEventAttributes']['markerName']

                if marker_name == 'cache':
                    details = e['markerRecordedEventAttributes']['details']
                    marker = Marker(name=marker_name, details=details)
                    cache_markers.append(marker)

        # Reset the context
        SwfDecisionContext.reset()
        SwfDecisionContext.decisions = Layer1Decisions()

        # Handle task retry statuses
        activities_with_retries = OrderedDict(activities)
        json_serializer = JsonSerializer()
        ids_to_delete = list()
        for activity in activities_with_retries.itervalues():
            if activity.control:
                control_data = json_serializer.deserialize(activity.control)
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
        SwfDecisionContext.activities = activities
        SwfDecisionContext.activities_iter = iter(activities_with_retries.itervalues())
        SwfDecisionContext.decision_task = decision_task
        SwfDecisionContext.signals = signals
        SwfDecisionContext.signals_iter = iter(signals)
        SwfDecisionContext.timers = timers
        SwfDecisionContext.timers_iter = iter(timers.itervalues())
        SwfDecisionContext.swf_history = history
        SwfDecisionContext.cache_markers = cache_markers
        SwfDecisionContext.cache_markers_iter = iter(cache_markers)
        SwfDecisionContext.workflow = workflow

