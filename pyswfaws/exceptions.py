class ActivityTaskException(Exception):
    """
    Exception that is thrown when an activity task fails
    """

    def __init__(self, task_name, task_version, task_id, failure_reason, failure_status):
        self.task_name = task_name
        self.task_version = task_version
        self.task_id = task_id
        self.failure_reason = failure_reason
        self.failure_stats = failure_status

    def __str__(self):
        return 'Task id {} ({}: {}) failed.  Reason: {}  Status: {}'.format(self.task_id, self.task_name,
                                                                            self.task_version, self.failure_reason,
                                                                            self.failure_stats)
