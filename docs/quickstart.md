# Quickstart

This guide will take you through creating a very simple decider and decision worker.  It's meant to serve as a gentle introduction

## Get the Library

```
pip install pyswfaws
```

## Create Files for our Code
We want to keep the activity workers and decision workers in separate files to make it easier to run them as separate
 processes.  Go ahead and create two files:  `activity.py` and `decision.py`.  Then, add the following to the top of 
 both files:

```
from datastores import *
from serializers import *
from decorators import *
from activityworker import *
from decisionworker import *
```

## Create an Activity Worker
We're going to create a simple activity function inside of `activity.py`:

``` 
@activity_task(swf_domain='example', swf_task_type='TestActivityA', swf_task_version='1.0',
               swf_task_list='activity_a_unit_test',
               input_data_serializer=JsonSerializer(), input_data_store=SwfDataStore(),
               result_data_serializer=JsonSerializer(), result_data_store=SwfDataStore())
def activity_task_a(a):
    return a * a
```

Our code for the activity task is complete, but now we need to add code to run it in distributed mode.  Append the 
following to the bottom of the file:

```
if __name__ == "__main__":
    activity_task_a_runner = DistributedActivityWorker(activity_task_a)
    activity_task_a_runner.start()
```

If this file is run from the command line, this activity worker will run in distributed mode and start communicating 
with SWF.  In order for this to work, you need a few things in place:

* A domain named `example` must be registered in SWF.
* An activity type named `TestActivityA` with version `1.0` must be registered in the example domain in SWF.
* A AWS credentials file must be created.  For more information, see this `AWS blog article <https://blogs.aws.amazon
.com/security/post/Tx3D6U6WSFGOK2H/A-New-and-Standardized-Way-to-Manage-Credentials-in-the-AWS-SDKs>`_.

## Create a Decision Worker
We're going to create a simple decision function inside of `decision.py`:

```
@decision_task(swf_domain='example', swf_workflow_type='TestWorkflow', swf_workflow_version='1.0',
               swf_task_list='unit_test_a',
               input_data_store=SwfDataStore(), input_data_serializer=JsonSerializer(),
               result_data_store=SwfDataStore(), result_data_serializer=JsonSerializer())
def decider_a():
    a1 = activity_task_a(5)
    a2 = activity_task_a(10)
    return a1.result + a2.result
```

Our code for the decision task is complete, but now we need to add code to run it in distributed mode.  Append the 
following to the bottom of the file:

```
if __name__ == "__main__":
    decision_task_a_runner = DistributedDecisionWorker(decision_task_a)
    decision_task_a_runner.start()
```

If this file is run from the command line, this activity worker will run in distributed mode and start communicating 
with SWF.  In order for this to work, you need a few things in place:

* A domain named `example` must be registered in SWF.
* A workflow type named `TestWorkflow` with version `1.0` must be registered in the example domain in SWF.
* A AWS credentials file must be created.  For more information, see this `AWS blog article <https://blogs.aws.amazon
.com/security/post/Tx3D6U6WSFGOK2H/A-New-and-Standardized-Way-to-Manage-Credentials-in-the-AWS-SDKs>`_.

## Running the Workflow
First, the two processes must be started.  In one terminal window, run:

```
python activity.py
```

and in another, run:

```
python decision.py
```

You now have an activity worker and decision worker waiting for work to do.  In order to start a workflow, go to the 
SWF console, navigate to the `example` domain (you should have created this when registering the workflow and activity 
types), navigate to `Workflow Types`, select `TestWorkflow`, and click `Start New Execution`.  No arguments are 
necessary, so you should be able to click through the dialog boxes and start the workflow.

Sit back, relax, and watch the workflow history grow as the tasks complete!