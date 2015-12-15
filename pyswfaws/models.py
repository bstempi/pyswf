"""
Defines models that help better represent SWF events.
"""

from boto.swf.layer1_decisions import Layer1Decisions


class Activity(object):
    """
    Represents the current state and history of a particular activity that was scheduled.
    """

    __slots__ = ['id', 'state', 'result', 'input', 'control', 'cancelling_decision', 'scheduling_decision',
                 'cancellation_details', 'failure_details', 'failure_reason', 'type', 'version',
                 'schedule_failure_cause', 'time_out_details', 'time_out_type', 'heartbeat_timeout',
                 'schedule_to_start_timeout', 'start_to_close_timeout', 'task_list', 'task_priority', 'attempt']

    def __init__(self, **kwargs):
        for slot in self.__slots__:
            setattr(self, slot, None)
        for k, v in kwargs.iteritems():
            setattr(self, k, v)


class Lambda(object):
    """
    Represents the current state and history of a particular lambda
    """

    __slots__ = ['id', 'input', 'name', 'failure_details', 'failure_reason', 'state']

    def __init__(self, **kwargs):
        for slot in self.__slots__:
            setattr(self, slot, None)
        for k, v in kwargs.iteritems():
            setattr(self, k, v)


class Marker(object):
    """
    Represents a marker
    """

    __slots__ = ['details', 'name']

    def __init__(self, **kwargs):
        for slot in self.__slots__:
            setattr(self, slot, None)
        for k, v in kwargs.iteritems():
            setattr(self, k, v)


class Signal(object):
    """
    Represents a signal sent to or recieved by a workflow
    """

    __slots__ = ['input', 'run_id', 'control', 'name', 'workflow_id']

    def __init__(self, **kwargs):
        for slot in self.__slots__:
            setattr(self, slot, None)
        for k, v in kwargs.iteritems():
            setattr(self, k, v)


class SwfDecisionContext(object):
    """
    Represents the context of a decision task.

    This object contains data about a decision task, such as the history.  Mere mortals should not be playing with
    this class.
    """

    __slots__ = ['decision_task', 'swf_history', 'decisions', 'activities', 'activities_iter', 'child_workflows',
                 'child_workflows_iter', 'cache_markers', 'cache_markers_iter', 'user_markers',
                 'user_markers_iter', 'signals', 'signals_iter', 'timers', 'timers_iter', 'workflow', '_id_generator']

    def __init__(self, **kwargs):
        for slot in self.__slots__:
            setattr(self, slot, None)
        for k, v in kwargs.iteritems():
            setattr(self, k, v)

        # No matter what, make sure we have a fresh deicions object
        if self.decisions is None:
            self.decisions = Layer1Decisions()
            self._id_generator = 0

    def get_next_id(self):
        """
        Generates ids in a deterministic way as strings for use in setting activity and child workflow ids.

        :return: a unique string id
        """
        self._id_generator += 1
        return str(self._id_generator)


class Timer(object):
    """
    Represents a timer object in SWF.
    """

    __slots__ = ['control', 'start_to_fire_timeout', 'id', 'state', 'failure_cause']

    def __init__(self, **kwargs):
        for slot in self.__slots__:
            setattr(self, slot, None)
        for k, v in kwargs.iteritems():
            setattr(self, k, v)


class Workflow(object):
    """
    Represents the current state and history of a particular child workflow
    """

    __slots__ = ['type', 'version', 'run_id', 'workflow_id', 'result', 'cancellation_details', 'failure_details',
                 'failure_reason', 'state', 'input', 'control', 'lambda_role', 'tag_list', 'task_list',
                 'task_priority', 'task_start_to_close_timeout', 'child_policy', 'continued_execution_run_id',
                 'parent_workflow_id', 'parent_run_id', 'version', 'failure_details', 'failure_reason',
                 'execution_start_to_close_timeout']

    def __init__(self, **kwargs):
        for slot in self.__slots__:
            setattr(self, slot, None)
        for k, v in kwargs.iteritems():
            setattr(self, k, v)


