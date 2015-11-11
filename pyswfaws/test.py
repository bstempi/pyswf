from multiprocessing import Process
from nose.plugins.attrib import attr
from datastores import *
from serializers import *
from decorators import *
from activityworker import *
from decisionworker import *

import unittest
import time

import boto


@decision_task(swf_domain='example', swf_workflow_type='TestWorkflow', swf_workflow_version='1.0',
               swf_task_list='unit_test',
               input_data_store=SwfDataStore(), input_data_serializer=JsonSerializer(),
               result_data_store=SwfDataStore(), result_data_serializer=JsonSerializer())
def decider_a():
    a = activity_task_a(5)
    b = activity_task_b(10)
    return a.result + b.result


@activity_task(swf_domain='example', swf_task_type='TestActivityA', swf_task_version='1.0',
               swf_task_list='activity_a_unit_test',
               input_data_serializer=JsonSerializer(), input_data_store=SwfDataStore(),
               result_data_serializer=JsonSerializer(), result_data_store=SwfDataStore())
def activity_task_a(a):
    return a * a


@activity_task(swf_domain='example', swf_task_type='TestActivityB', swf_task_version='1.0',
               swf_task_list='activity_b_unit_test',
               input_data_serializer=JsonSerializer(), input_data_store=SwfDataStore(),
               result_data_serializer=JsonSerializer(), result_data_store=SwfDataStore())
def activity_task_b(b):
    return b + b

# @attr('integration')
class LiveSwfWorkflowTest(unittest.TestCase):
    """
    Test that runs a workflow in SWF
    """

    @staticmethod
    def start_decisioner_a():
        decision_task_a_runner = DistributedDecisionWorker(decider_a)
        decision_task_a_runner.start()

    @staticmethod
    def start_activity_worker_a():
        activity_task_a_runner = DistributedActivityWorker(activity_task_a)
        activity_task_a_runner.start()

    @staticmethod
    def start_activity_worker_b():
        activity_task_b_runner = DistributedActivityWorker(activity_task_b)
        activity_task_b_runner.start()

    @classmethod
    def setUpClass(cls):
        LiveSwfWorkflowTest.swf = boto.connect_swf()

        LiveSwfWorkflowTest.activity_task_a_process = Process(target=LiveSwfWorkflowTest.start_activity_worker_a)
        LiveSwfWorkflowTest.activity_task_b_process = Process(target=LiveSwfWorkflowTest.start_activity_worker_b)
        LiveSwfWorkflowTest.decision_task_a_process = Process(target=LiveSwfWorkflowTest.start_decisioner_a)

        LiveSwfWorkflowTest.activity_task_a_process.start()
        LiveSwfWorkflowTest.activity_task_b_process.start()
        LiveSwfWorkflowTest.decision_task_a_process.start()
        time.sleep(10)

    @classmethod
    def tearDownClass(cls):
        LiveSwfWorkflowTest.activity_task_a_process.terminate()
        LiveSwfWorkflowTest.activity_task_b_process.terminate()
        LiveSwfWorkflowTest.decision_task_a_process.terminate()

    def test_simple_workflow(self):
        # TODO Test output of workflow, not just the status
        wf_result = LiveSwfWorkflowTest.swf.start_workflow_execution(domain='example', workflow_id='unit_test',
                                                                     workflow_name='TestWorkflow',
                                                                     workflow_version='1.0',
                                                                     task_list='unit_test')
        if wf_result:
            run_id = wf_result['runId']
            while True:
                time.sleep(5)
                wf_result = LiveSwfWorkflowTest.swf.describe_workflow_execution(domain='example', run_id=run_id,
                                                                                workflow_id='unit_test')
                if wf_result['executionInfo'].get('closeStatus') is not None:
                    break
            if wf_result['executionInfo'].get('closeStatus') != 'COMPLETED':
                self.fail('Workflow {} failed'.format(wf_result['executionInfo']['execution']['runId']))
        else:
            self.fail('Error when launching workflow')
