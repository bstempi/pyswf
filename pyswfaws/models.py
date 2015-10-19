"""
Defines models that help better represent SWF events.
"""


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

    This object contains data about a decision task, such as the history.  It also contains the "mode" of the
    operation (e.g., running locally, distributed, etc.

    The members of this class are meant to be used in a static fashion.  The intention is that there is some thread (
    A) that listens for decision tasks and some thread (B) that processes that decision via replay.  This class and
    it's static members are the data transfer mechanism between A and B.
    """

    Distributed, SerialLocal = range(2)
    mode = SerialLocal
    decision_task = None
    swf_history = None
    decisions = None
    activities = None
    activities_iter = None
    cache_markers = None
    cache_markers_iter = None
    signals = None
    signals_iter = None
    timers = None
    timers_iter = None
    workflow = None
    finished = False
    exception = None
    trace_back = None

    _id_generator = 0

    @staticmethod
    def reset():
        # DO NOT RESET THE MODE
        #self.mode = self.SerialLocal
        decision_task = None
        swf_history = None
        decisions = None
        activities = None
        activities_iter = None
        signals = None
        signals_iter = None
        timers = None
        timers_iter = None
        workflow = None
        finished = False
        output = None
        exception = None
        trace_back = None
        _id_generator = 0

    @staticmethod
    def get_next_id():
        SwfDecisionContext._id_generator += 1
        return str(SwfDecisionContext._id_generator)


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


