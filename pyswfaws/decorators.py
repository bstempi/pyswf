"""
Defines the decorators used to decorate activity and decision functions
"""

import inspect
import venusian
import uuid

from models import Activity, Workflow
from promise import DistributedActivityPromise, DistributedChildWorkflowPromise, Promise
from serializers import JsonSerializer


def activity_task(f, swf_domain=None, swf_task_type=None, swf_task_version=None, swf_task_list=None,
                  input_data_serializer=None, input_data_store=None, result_data_serializer=None,
                  result_data_store=None, max_attempts=5, retry_states=('FAILED_TO_SCHEDULE', 'FAILED', 'TIMED_OUT')):
    """
    Decorates a function as an activity task.

    The decorator does not change it's behavior unless it's being run by one of the special runners.  If it's being
    used within a decider (local or remote), then this will cause the function to return Promises instead of
    returning results directly.

    :param f:
    :param swf_domain:
    :param swf_task_type:
    :param swf_task_version:
    :param swf_task_list:
    :param input_data_serializer:
    :param input_data_store:
    :param result_data_serializer:
    :param result_data_store:
    :param max_attempts:
    :param retry_states:
    :return:
    """
    def callback(scanner, name, ob):
        """
        Uses the scanner to determine the mode of operation, and thus which wrapper should be used.
        :param scanner:
        :param name:
        :param ob:
        :return:
        """

        def decider_local_activity_task(*args, **kwargs):
            """
            Runs the activity task in decider-local mode.

            :param args:
            :param kwargs:
            :return:
            """
            p = Promise()
            p.is_ready = True
            try:
                result = f(*args, **kwargs)
                p.result = result
            except Exception as e:
                p.exception = e
            return e

        def decider_remote_activity_task(*args, **kwargs):
            """
            Runs the activity task in decider-remote mode.

            :param args:
            :param kwargs:
            :return:
            """
            if scanner.control_serializer is None:
                scanner.control_serializer = JsonSerializer()

            decision_context = scanner.decision_context
            control_serializer = scanner.control_serializer

            try:
                activity = decision_context.activities_iter.next()

                # Do we need to schedule a retry?
                if activity.state in retry_states:
                    attempts = 1
                    original_task_id = activity.id
                    control_data = {}
                    if activity.control is not None:
                        control_data = scanner.control_serializer.deserialize_result(activity.control)
                        attempts = control_data['attempts']
                        original_task_id = control_data['original_attempt_task_id']
                    # Do we have any attempts left?
                    if attempts < max_attempts:
                        control_data['attempts'] = attempts + 1
                        control_data['original_attempt_task_id'] = original_task_id
                        decision_context.decisions.schedule_activity_task(activity_id=decision_context.get_next_id(),
                                                                          activity_type_name=activity.type,
                                                                          activity_type_version=activity.version,
                                                                          task_list=activity.task_list,
                                                                          input=activity.input,
                                                                          control=control_serializer.serialize_result(control_data))
                        # If we had to schedule a retry, then we need something to return to the user, so we create a
                        # promise based around a fake activity
                        activity_stub = Activity(state='SCHEDULED')
                        return DistributedActivityPromise(activity=activity_stub)
                # Whether or not we have a failure on our hands, if we're not retrying, then we're returning a
                # promise to the user based on the activity we got back.

                # Deserialize result
                if activity.result:
                    serialized_result = result_data_store.get(activity.result)
                    activity.result = result_data_serializer.deserialize_result(serialized_result)
                return DistributedActivityPromise(activity=activity)
            # Do we have a new activity to schedule?
            except StopIteration:
                # We have one of two branches here:  either we have a function, or we have a method.  We have to be
                # careful with methods because we don't want to serialize `self`.
                if inspect.ismethod(f):
                    # Ok, it's a method.  So, where's self?  is it in kwargs or args?
                    if 'self' in kwargs:
                        # Save the self somewhere just in case
                        method_self = kwargs['self']
                        del kwargs['self']
                    else:
                        method_self = args.pop(0)

                # By this point, we've assured that args and kwargs are save for serialization
                serialized_input = input_data_serializer.serialize_input(args, kwargs)
                task_id = decision_context.get_next_id()
                key = '{}-task-{}'.format(swf_task_type, task_id)
                swf_input = input_data_store.put(serialized_input, key)
                decision_context.decisions.schedule_activity_task(activity_id=task_id,
                                                                  activity_type_name=swf_task_type,
                                                                  activity_type_version=swf_task_version,
                                                                  task_list=swf_task_list,
                                                                  input=swf_input)
                activity = Activity(state='SCHEDULED')
                return DistributedActivityPromise(activity)

        # Get the mode, defaulting to local mode.
        mode = getattr(scanner, 'mode', 'local')
        caller = getattr(scanner, 'caller', None)
        if mode == 'local' and caller is None:
            # We do nothing; just pass through the call
            return f

        if caller == 'activity_worker':
            # It doesn't matter what mode we're in; activity workers can't invoke each other in SWF, so there's no
            # to do any promise trickery.  Just pass it through.
            return f

        if mode == 'local' and caller == 'decision_worker':
            # We treat this as a serial call from the decider
            scanner.registry.add(name, decider_local_activity_task)
        elif mode == 'remote' and caller == 'decision_worker':
            # We don't want to call the actual function at all; use the remote handler

            # Set some function properties for the runners to discover
            # Better to store this data as function properties vs storing it in the scanner since everyone can read
            # the function properties.  Presumably, only few will have access to the scanner.
            decider_remote_activity_task.swf_options = dict()
            decider_remote_activity_task.swf_options['domain'] = swf_domain
            decider_remote_activity_task.swf_options['task_type'] = swf_task_type
            decider_remote_activity_task.swf_options['task_version'] = swf_task_version
            decider_remote_activity_task.swf_options['task_list'] = swf_task_list

            decider_remote_activity_task.retry_options = dict()
            decider_remote_activity_task.retry_options['max_attempts'] = max_attempts
            decider_remote_activity_task.retry_options['retry_states'] = retry_states

            decider_remote_activity_task.serialization_options = dict()
            decider_remote_activity_task.serialization_options['input_serializer'] = input_data_serializer
            decider_remote_activity_task.serialization_options['input_data_store'] = input_data_store
            decider_remote_activity_task.serialization_options['result_serializer'] = result_data_serializer
            decider_remote_activity_task.serialization_options['result_data_store'] = result_data_store

            scanner.registry.add(name, decider_remote_activity_task)
    venusian.attach(f, callback, category='pyswfaws.activity_task')
    return f


def decision_task(f, swf_domain=None, swf_workflow_type=None, swf_workflow_version=None, swf_task_list=None,
                  input_data_serializer=None, input_data_store=None, result_data_serializer=None,
                  result_data_store=None, continues_indefinitely=False):
    """
    Decorates a function as a decision task.

    The decorator does not change it's behavior unless it's being run by one of the special runners.  If it's being
    used within a decider (local or remote), then this will cause the function to return Promises instead of
    returning results directly.

    :param f:
    :param swf_domain:
    :param swf_workflow_type:
    :param swf_workflow_version:
    :param swf_task_list:
    :param input_data_serializer:
    :param input_data_store:
    :param result_data_serializer:
    :param result_data_store:
    :param continues_indefinitely:
    :return:
    """
    def callback(scanner, name, ob):
        """
        Uses the scanner to determine the mode of operation, and thus which wrapper should be used.
        :param scanner:
        :param name:
        :param ob:
        :return:
        """
        def decider_local_child_workflow(*args, **kwargs):
            """
            Treats this decision task as a child workflow within another decision task in a serial fashion.
            :param args:
            :param kwargs:
            :return:
            """
            p = Promise()
            p.is_ready = True
            try:
                result = f(*args, **kwargs)
                p.result = result
            except Exception as e:
                p.exception = e
            return e

        def decider_remote_child_workflow(*args, **kwargs):
            """
            Treats this decision task as if it were a child workflow in remote mode.
            :param args:
            :param kwargs:
            :return:
            """

            decision_context = scanner.decision_context

            try:
                cwf = decision_context.child_workflows_iter.next()

                # Deserialize results before returning the promise
                if cwf.result:
                    serialized_result = result_data_store.get(cwf.result)
                    cwf.result = result_data_serializer.deserialize_result(serialized_result)
                return DistributedChildWorkflowPromise(cwf)

            # Do we have a new activity to schedule?
            except StopIteration:
                # We have one of two branches here:  either we have a function, or we have a method.  We have to be
                # careful with methods because we don't want to serialize `self`.
                if inspect.ismethod(f):
                    # Ok, it's a method.  So, where's self?  is it in kwargs or args?
                    if 'self' in kwargs:
                        # Save the self somewhere just in case
                        method_self = kwargs['self']
                        del kwargs['self']
                    else:
                        method_self = args.pop(0)

                # By this point, we've assured that args and kwargs are save for serialization
                serialized_input = input_data_serializer.serialize_input(args, kwargs)
                task_id = decision_context.get_next_id()
                key = '{}-cwf-{}'.format(swf_workflow_type, task_id)
                swf_input = input_data_store.put(serialized_input, key)
                decision_context.decisions.start_child_workflow_execution(workflow_type_name=swf_workflow_type,
                                                                          workflow_type_version=swf_workflow_version,
                                                                          input=swf_input, task_list=swf_task_list)
                cwf = Workflow(state='SCHEDULED')
                return DistributedChildWorkflowPromise(cwf)

        # Get the mode, defaulting to local mode.
        mode = getattr(scanner, 'mode', 'local')
        caller = getattr(scanner, 'caller', None)
        parent_decision_worker = getattr(scanner, 'parent_decision_worker', None)

        # IF this was called by a decision worker and this function isn't the decision task that's runnign the
        # workflow, then we have a child workflow
        if caller == 'decision_worker' and parent_decision_worker != ob:
            if mode == 'remote':
                scanner.registry.add(name, decider_remote_child_workflow)
            else:
                scanner.registry.add(name, decider_local_child_workflow)
        # Just attach meta data to f and be done with
        else:
            f.swf_options = dict()
            f.swf_options['domain'] = swf_domain
            f.swf_options['wf_type'] = swf_workflow_type
            f.swf_options['wf_version'] = swf_workflow_version
            f.swf_options['task_list'] = swf_task_list

            f.serialization_options = dict()
            f.serialization_options['input_serializer'] = input_data_serializer
            f.serialization_options['input_data_store'] = input_data_store
            f.serialization_options['result_serializer'] = result_data_serializer
            f.serialization_options['result_data_store'] = result_data_store

    venusian.attach(f, callback, category='pyswfaws.decision_task')
    return f


def cached(f, result_data_store, result_data_serializer):
    """
    Decorates a function as something that should be cached.

    This decorator does not change the behavior of a function unless it is being run within a decisioner.  When run
    in remote mode, this decorator will run the function once and store it's result, using that initial return value
    as the return value for future calls.  Local mode will do the same thing, but with a dict.
    :param f:
    :return:
    """
    def callback(scanner, name, ob):
        """
        Uses the scanner to determine the mode of operation, and thus which wrapper should be used.

        :param scanner:
        :param name:
        :param ob:
        :return:
        """

        saved_results = dict()

        def cache_result_in_marker(*args, **kwargs):
            decision_context = scanner.decision_context

            # First, see if we have any cache markers and try to return the result from there
            try:
                cached_result = decision_context.cache_markers_iter.next()
                result = None
                if cached_result.details:
                    serialized_result = result_data_store.get(cached_result.details)
                    result = result_data_serializer.deserialize_result(serialized_result)
                return result

            # If we're in the except body, then this is a result that's never been run/cached before
            except StopIteration:

                # Run the method
                result = f(*args, **kwargs)

                # Serialize the result
                if result:
                    serialized_result = result_data_serializer.serialize_result(result)
                    key = '{}-{}'.format(decision_context.workflow.run_id, str(uuid.uuid4()))
                    data_store_key = result_data_store.put(serialized_result, key)

                # Store a marker in SWF
                decision_context.decisions.record_marker('cache', data_store_key)

                # Give the result back to the caller
                return result

        def cache_result_locally(*args, **kwargs):
            caller = ob.__name__

            # Put it in the map if it's not already there
            if caller not in saved_results:
                result = f(*args, **kwargs)
                saved_results[caller] = result

            return saved_results[caller]

        # Get the mode, defaulting to local mode.
        mode = getattr(scanner, 'mode', 'local')
        caller = getattr(scanner, 'caller', None)

        if mode == 'remote' and caller == 'decision_worker':
            scanner.registry.add(name, cache_result_in_marker())

    venusian.attach(f, callback, category='pyswfaws.decision_task')
    return f


def nondeterministic(f, result_data_store, result_data_serializer):
    """
    Alias for cached()
    :param f:
    :param result_data_store:
    :param result_data_serializer:
    :return:
    """
    cached(f, result_data_store, result_data_serializer)