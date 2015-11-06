import boto
import logging
import venusian
import sys


class DistributedActivityWorker(object):
    """
    This class is the base of all activity workers.  It is responsible for the event loop, message serialization, and
    message storage.  Users are expected to extend this class and to provide an implementation of handle_task to carry
    out their work.
    """

    logger = logging.getLogger('pyswfaws.ActivityWorker')

    def __init__(self, activity_function, swf_domain=None, swf_task_list=None, activity_type=None,
                 activity_version=None, aws_access_key_id=None, aws_secret_access_key=None):
        """
        Inits an activity worker

        A few things to note:
        * If no AWS credentials are passed in, then it will be assumed that they should be sought after by Boto.  The
        places that Boto uses and their order of precedence can be found here:  http://boto.readthedocs.org/en/latest/boto_config_tut.html
        * If one of the swf_ arguments are left None, the constructor will fall back to using the data supplied by
        the @decision_task decorator.

        :param activity_function: the activity function being used
        :param swf_domain: SWF domain used by this worker
        :param swf_task_list: SWF task list that this worker is listening to
        :param aws_access_key_id: Access key to use for S3 and SWF.  If none is supplied, boto will fallback to looking for credentials elsewhere.
        :param aws_secret_access_key: Secret key to use for S3 and SWF.  If none is supplied, boto will fallback to looking for credentials elsewhere.
        :return:
        """

        self._swf = boto.connect_swf(aws_access_key_id=aws_access_key_id,
                                     aws_secret_access_key=aws_secret_access_key)
        self._activity_function = activity_function

        registry = Registry()
        scanner = venusian.Scanner(registry=registry, mode='remote', caller='activity_worker')

        # Some trickery here -- scan the module that the activity worker method is found in
        scanner.scan(sys.modules[activity_function.__module__])

        if hasattr(activity_function, 'swf_options'):

            # Give preference to the values in the constructor
            self._swf_domain = self.choose_first_not_none('An SWF domain must be specified by the activity worker '
                                                          'constructor or the activity function', swf_domain,
                                                          activity_function.swf_options['domain'])
            self._swf_task_list = self.choose_first_not_none('An SWF task list must be specified by the activity '
                                                             'worker constructor or the activity function',
                                                             swf_task_list, activity_function.swf_options['task_list'])
            self._activity_type = self.choose_first_not_none('An SWF activity type must be specified by the activity '
                                                             'worker constructor or the activity function',
                                                             swf_task_list, activity_function.swf_options['task_type'])
            self._activity_version = self.choose_first_not_none('An SWF activity version must be specified by the '
                                                                'activity worker constructor or the activity function',
                                                                activity_version, activity_function.swf_options[
                                                                    'task_version'])

            self._input_serializer = activity_function.serialization_options['input_serializer']
            self._input_data_store = activity_function.serialization_options['input_data_store']
            self._result_serializer = activity_function.serialization_options['result_serializer']
            self._result_data_store = activity_function.serialization_options['result_data_store']
        else:
            raise Exception('Activity function has no "swf_options" attribute; are you sure the function was properly '
                            'decorated?')

    def handle_no_op(self, activity_task):
        """
        Optional method for intercepting polls that did not receive a task
        :return:
        """
        pass

    def start(self):
        """
        Starts the event loop.  This method blocks and runs infinitely.
        :return:
        """
        while True:
            self.logger.debug('Polling')
            activity_task = self._swf.poll_for_activity_task(domain=self._swf_domain, task_list=self._swf_task_list)

            # No-op if we got no events
            if 'activityId' not in activity_task:
                self.logger.debug('Calling the no-op handler')
                self.handle_no_op(activity_task)
                continue

            try:
                input = (list(), dict())
                if 'input' in activity_task and activity_task['input'] != '':
                    self.logger.debug('Unpacking input message')
                    serialized_input = self._input_data_store.get(activity_task['input'])
                    input = self._input_serializer.deserialize_input(serialized_input)

                # Let the user handle it
                self.logger.debug("Calling the user's handler")

                # In this case, result will be a promise.  Be sure to treat it as such.
                result = self._activity_functionc(*input[0], **input[1])

                swf_result = None
                if result.result:
                    self.logger.debug('Serializing activity result')
                    serialized_result = self._result_serializer.serialize_result(result.result)
                    key = '{}-{}'.format(activity_task['workflowExecution']['runId'], activity_task['activityId'])
                    swf_result = self._result_data_store.put(serialized_result, key)

                self.logger.debug('Marking activity as completed')
                self._swf.respond_activity_task_completed(activity_task['taskToken'], swf_result)

            except Exception as e:
                self.logger.debug('Calling the exception handler')
                should_exit = self.handle_exception(e, activity_task)
                if should_exit:
                    self.logger.debug('Exiting due to return value from handle_exception()')
                    return

    @staticmethod
    def choose_first_not_none(exception_message, *args):
        for arg in args:
            if arg is not None:
                return arg
        raise Exception(exception_message)


class Registry(object):
    def __init__(self):
        self.registered = []

    def add(self, name, ob):
        self.registered.append((name, ob))
