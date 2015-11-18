"""
Defines the decorators used to decorate activity and decision functions
"""

import inspect
import types
import uuid

import venusian

from models import Activity, Workflow
from promise import DistributedActivityPromise, DistributedChildWorkflowPromise, Promise
from serializers import JsonSerializer


def activity_task(swf_domain=None, swf_task_type=None, swf_task_version=None, swf_task_list=None,
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
    def wrapper(f):
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
                    p.result = f(*args, **kwargs)
                except Exception as e:
                    p.exception = e
                return p

            def decider_remote_activity_task(*args, **kwargs):
                """
                Runs the activity task in decider-remote mode.

                :param args:
                :param kwargs:
                :return:
                """
                if not hasattr(scanner, 'control_serializer'):
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

            # Just in case we're doing multiple scans per process, we need to make sure ob is properly reset.
            ob.decorated = None

            if mode == 'local' and caller == 'decision_worker':
                # We treat this as a serial call from the decider
                scanner.registry.add(ob, decider_local_activity_task)
                ob.decorated = decider_local_activity_task
            elif mode == 'remote' and caller == 'decision_worker':
                # We don't want to call the actual function at all; use the remote handler
                scanner.registry.add(ob, decider_remote_activity_task)
                ob.decorated = decider_remote_activity_task

        # Make a copy of our dynamic function so that every function that we return has independent attributes
        d_f_copy = DynamicCallableFunction(original_function=f)

        # Setting properties on our returned function
        d_f_copy.swf_options = dict()
        d_f_copy.swf_options['domain'] = swf_domain
        d_f_copy.swf_options['task_type'] = swf_task_type
        d_f_copy.swf_options['task_version'] = swf_task_version
        d_f_copy.swf_options['task_list'] = swf_task_list

        d_f_copy.retry_options = dict()
        d_f_copy.retry_options['max_attempts'] = max_attempts
        d_f_copy.retry_options['retry_states'] = retry_states

        d_f_copy.serialization_options = dict()
        d_f_copy.serialization_options['input_serializer'] = input_data_serializer
        d_f_copy.serialization_options['input_data_store'] = input_data_store
        d_f_copy.serialization_options['result_serializer'] = result_data_serializer
        d_f_copy.serialization_options['result_data_store'] = result_data_store

        # Venusian magic
        venusian.attach(d_f_copy, callback, category='pyswfaws.activity_task')

        return d_f_copy
    return wrapper


def decision_task(swf_domain=None, swf_workflow_type=None, swf_workflow_version=None, swf_task_list=None,
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
    def wrapper(f):
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
                return p

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
                    workflow_id = 'wf-{}'.format(uuid.uuid4())
                    decision_context.decisions.start_child_workflow_execution(workflow_type_name=swf_workflow_type,
                                                                              workflow_type_version=swf_workflow_version,
                                                                              input=swf_input, task_list=swf_task_list,
                                                                              workflow_id=workflow_id)
                    cwf = Workflow(state='SCHEDULED')
                    return DistributedChildWorkflowPromise(cwf)

            # Get the mode, defaulting to local mode.
            mode = getattr(scanner, 'mode', 'local')
            caller = getattr(scanner, 'caller', None)
            parent_decision_worker = getattr(scanner, 'parent_decision_worker', None)

            # Just in case we're doing multiple scans per process, we need to make sure that ob is properly reset.
            ob.decorated = None

            # IF this was called by a decision worker and this function isn't the decision task that's running the
            # workflow, then we have a child workflow
            if caller == 'decision_worker' and parent_decision_worker != ob:
                if mode == 'remote':
                    scanner.registry.add(ob, decider_remote_child_workflow)
                    ob.decorated = decider_remote_child_workflow
                else:
                    scanner.registry.add(ob, decider_local_child_workflow)
                    ob.decorated = decider_local_child_workflow

        wrapped_f = DynamicCallableFunction(original_function=f)
        wrapped_f.swf_options = dict()
        wrapped_f.swf_options['domain'] = swf_domain
        wrapped_f.swf_options['wf_type'] = swf_workflow_type
        wrapped_f.swf_options['wf_version'] = swf_workflow_version
        wrapped_f.swf_options['task_list'] = swf_task_list

        wrapped_f.serialization_options = dict()
        wrapped_f.serialization_options['input_serializer'] = input_data_serializer
        wrapped_f.serialization_options['input_data_store'] = input_data_store
        wrapped_f.serialization_options['result_serializer'] = result_data_serializer
        wrapped_f.serialization_options['result_data_store'] = result_data_store

        # Venusian magic
        venusian.attach(wrapped_f, callback, category='pyswfaws.decision_task')

        return wrapped_f
    return wrapper


def cached(result_data_store, result_data_serializer):
    """
    Decorates a function as something that should be cached.

    This decorator does not change the behavior of a function unless it is being run within a decisioner.  When run
    in remote mode, this decorator will run the function once and store it's result, using that initial return value
    as the return value for future calls.  Local mode will do the same thing, but with a dict.
    :param result_data_serializer:
    :param result_data_store:
    :return:
    """
    def wrapped(f):
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

            # Get the mode, defaulting to local mode.
            mode = getattr(scanner, 'mode', 'local')
            caller = getattr(scanner, 'caller', None)

            if mode == 'remote' and caller == 'decision_worker':
                scanner.registry.add(ob, cache_result_in_marker)
                ob.decorated = cache_result_in_marker

        wrapped_f = DynamicCallableFunction(f)
        venusian.attach(wrapped_f, callback, category='pyswfaws.decision_task')
        return wrapped_f
    return wrapped


def nondeterministic(f, result_data_store, result_data_serializer):
    """
    Alias for cached()
    :param f:
    :param result_data_store:
    :param result_data_serializer:
    :return:
    """
    cached(f, result_data_store, result_data_serializer)


class DynamicCallableFunction(object):
    """
    Callable object that acts as a dynamic function

    When the decorator is first read, the original function is thinly wrapped with this class and passed
    through.  The idea is that later on when we do a Venucian scan, we can modify that object to know which
    function it should be calling instead.  This is being done as a class because attaching state to
    functions is a pain.  Specifically, there's no equivalent of self for a function, so if we have copies of
    functions with unique attributes, it's hard to get to those attributes,
    """
    __slots__ = ['orig', 'decorated', 'swf_options', 'retry_options', 'serialization_options',
                 '__venusian_callbacks__']

    def __init__(self, original_function):
        if original_function is None:
            raise Exception('The original function must be passed in during construction and cannot be None')
        self.orig = original_function
        self.decorated = None

    def __call__(self, *args, **kwargs):
        if self.decorated is None:
            return self.orig(*args, **kwargs)
        return self.decorated(*args, **kwargs)
