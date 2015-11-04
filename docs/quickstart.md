# Quickstart

This guide will take you through creating a very simple decider and decision worker.  It's meant to serve as a gentle introduction

## Get the Library

```
pip install pyswfaws
```

## Create an Activity Worker
Put this code into a file named `activity.py`.

``` 
from pyswfaws.datastores import SwfDataStore
from pyswfaws.serializers import JsonSerializer
from pyswfaws.activityworker import ActivityWorker
from pyswfaws.models import SwfDecisionContext

class SimpleActivityWorker(ActivityWorker):
    """
    A simple activity worker that just squares a number
    """
    class Meta:
        swf_domain = 'example'
        swf_task_list = 'simple-activity'
        activity_type = 'simple-activity-worker'
        activity_version = '1.0'
        input_serializer = JsonSerializer()
        input_data_store = SwfDataStore()
        result_serializer = JsonSerializer()
        result_data_store = SwfDataStore()

    def __init__(self):
        super(SimpleActivityWorker, self).__init__()

    @ActivityWorker.activity_task
    def handle_task(self, a):
        return a * a
```

## Create a Decision Worker
Put this code into a file named `decision.py`.

```
from pyswfaws.datastores import SwfDataStore
from pyswfaws.serializers import JsonSerializer
from decisionworker import DecisionWorker
from pyswfaws.models import SwfDecisionContext

from activity import SimpleActivityWorker

class SimpleDecisionWorker(DecisionWorker):
    """
    A simple activity worker that just squares a number
    """
    class Meta:
        swf_domain = 'example'
        swf_task_list = 'simple-activity'
        input_serializer = JsonSerializer()
        input_data_store = SwfDataStore()
        result_serializer = JsonSerializer()
        result_data_store = SwfDataStore()

    def __init__(self, mode):
        super(SimpleDecisionWorker, self).__init__(mode=mode)
        self.activity = SimpleActivityWorker()

    def handle(self, a):
        promise = self.activity.handle_task(a)
        
        return promise.result
```

## Run the Workflow in Serial Mode
TODO

## Run the Workflow in Distributed Mode
TODO