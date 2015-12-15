from multiprocessing import Process
from nose.plugins.attrib import attr
from datastores import *
from serializers import *
from decorators import *
from activityworker import *
from decisionworker import *
from promise import Timer as PTimer, Marker as PMarker

import unittest
import time
import timeit

import boto


# This is to allow logging during debugging/testing
logging.basicConfig(level=logging.DEBUG)


@decision_task(swf_domain='example', swf_workflow_type='TestWorkflow', swf_workflow_version='1.0',
               swf_task_list='unit_test_a',
               input_data_store=SwfDataStore(), input_data_serializer=JsonSerializer(),
               result_data_store=SwfDataStore(), result_data_serializer=JsonSerializer())
def decider_a():
    a = activity_task_a(5)
    b = activity_task_b(10)
    return a.result + b.result


@decision_task(swf_domain='example', swf_workflow_type='TestWorkflow', swf_workflow_version='1.0',
               swf_task_list='unit_test_b',
               input_data_store=SwfDataStore(), input_data_serializer=JsonSerializer(),
               result_data_store=SwfDataStore(), result_data_serializer=JsonSerializer())
def decider_b():
    a = activity_task_a(5)
    timer = PTimer.get(seconds=10)
    # Force WF to wait for the timer
    timer.result
    b = activity_task_b(10)
    return a.result + b.result


@decision_task(swf_domain='example', swf_workflow_type='TestWorkflow', swf_workflow_version='1.0',
               swf_task_list='unit_test_c',
               input_data_store=SwfDataStore(), input_data_serializer=JsonSerializer(),
               result_data_store=SwfDataStore(), result_data_serializer=JsonSerializer())
def decider_c():
    a = activity_task_a(5)
    child_workflow = decider_a()
    return a.result + child_workflow.result


@decision_task(swf_domain='example', swf_workflow_type='TestWorkflow', swf_workflow_version='1.0',
               swf_task_list='unit_test_d',
               input_data_store=SwfDataStore(), input_data_serializer=JsonSerializer(),
               result_data_store=SwfDataStore(), result_data_serializer=JsonSerializer())
def decider_d():
    a = activity_task_a(5)
    b = activity_task_b(10)
    c = some_cached_function()
    d = some_cached_function()
    e = some_cached_function()
    return a.result + b.result + c + d + e


@decision_task(swf_domain='example', swf_workflow_type='TestWorkflow', swf_workflow_version='1.0',
               swf_task_list='unit_test_f',
               input_data_store=SwfDataStore(), input_data_serializer=JsonSerializer(),
               result_data_store=SwfDataStore(), result_data_serializer=JsonSerializer())
def decider_f():
    a = activity_task_a(5)
    marker = PMarker(name='someMarker', details='someDetails')
    b = activity_task_b(10)

    return str(a.result) + str(b.result) + ' ' + marker.result.name + ' ' + marker.result.details


class SomeDecider(object):
    @decision_task(swf_domain='example', swf_workflow_type='TestWorkflow', swf_workflow_version='1.0',
    swf_task_list='unit_test_e',
    input_data_store=SwfDataStore(), input_data_serializer=JsonSerializer(),
    result_data_store=SwfDataStore(), result_data_serializer=JsonSerializer())
    def decider_e(self):
        a = activity_task_a(5)
        b = activity_task_b(10)
        return a.result + b.result


# TODO Currently unused; need to write a test for object-based activities
class SomeActivity(object):
    @activity_task(swf_domain='example', swf_task_type='TestWorkflow', swf_task_version='1.0',
               swf_task_list='unit_test_c',
               input_data_store=SwfDataStore(), input_data_serializer=JsonSerializer(),
               result_data_store=SwfDataStore(), result_data_serializer=JsonSerializer())
    def activity_c(self, a):
        return 3 * a


@cached(result_data_serializer=JsonSerializer(), result_data_store=SwfDataStore())
def some_cached_function():
    return 5


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


@attr('integration')
class LiveSwfWorkflowTest(unittest.TestCase):
    """
    Test that runs a workflow in SWF
    """

    @staticmethod
    def start_decisioner_a():
        decision_task_a_runner = DistributedDecisionWorker(decider_a)
        decision_task_a_runner.start()

    @staticmethod
    def start_decisioner_b():
        decision_task_b_runner = DistributedDecisionWorker(decider_b)
        decision_task_b_runner.start()

    @staticmethod
    def start_decisioner_c():
        decision_task_c_runner = DistributedDecisionWorker(decider_c)
        decision_task_c_runner.start()

    @staticmethod
    def start_decisioner_d():
        decision_task_d_runner = DistributedDecisionWorker(decider_d)
        decision_task_d_runner.start()

    @staticmethod
    def start_decisioner_e():
        decider_e = SomeDecider()
        decision_task_e_runner = DistributedDecisionWorker(decider_e.decider_e, decider_e)
        decision_task_e_runner.start()

    @staticmethod
    def start_decisioner_f():
        decision_task_f_runner = DistributedDecisionWorker(decider_f)
        decision_task_f_runner.start()

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
        LiveSwfWorkflowTest.decision_task_b_process = Process(target=LiveSwfWorkflowTest.start_decisioner_b)
        LiveSwfWorkflowTest.decision_task_c_process = Process(target=LiveSwfWorkflowTest.start_decisioner_c)
        LiveSwfWorkflowTest.decision_task_d_process = Process(target=LiveSwfWorkflowTest.start_decisioner_d)
        LiveSwfWorkflowTest.decision_task_e_process = Process(target=LiveSwfWorkflowTest.start_decisioner_e)
        LiveSwfWorkflowTest.decision_task_f_process = Process(target=LiveSwfWorkflowTest.start_decisioner_f)

        LiveSwfWorkflowTest.activity_task_a_process.start()
        LiveSwfWorkflowTest.activity_task_b_process.start()
        LiveSwfWorkflowTest.decision_task_a_process.start()
        LiveSwfWorkflowTest.decision_task_b_process.start()
        LiveSwfWorkflowTest.decision_task_c_process.start()
        LiveSwfWorkflowTest.decision_task_d_process.start()
        LiveSwfWorkflowTest.decision_task_e_process.start()
        LiveSwfWorkflowTest.decision_task_f_process.start()
        time.sleep(10)

    @classmethod
    def tearDownClass(cls):
        LiveSwfWorkflowTest.activity_task_a_process.terminate()
        LiveSwfWorkflowTest.activity_task_b_process.terminate()
        LiveSwfWorkflowTest.decision_task_a_process.terminate()
        LiveSwfWorkflowTest.decision_task_b_process.terminate()
        LiveSwfWorkflowTest.decision_task_c_process.terminate()
        LiveSwfWorkflowTest.decision_task_d_process.terminate()
        LiveSwfWorkflowTest.decision_task_e_process.terminate()
        LiveSwfWorkflowTest.decision_task_f_process.terminate()

    def test_simple_workflow(self):
        # TODO Test output of workflow, not just the status
        wf_result = LiveSwfWorkflowTest.swf.start_workflow_execution(domain='example', workflow_id='unit_test_a',
                                                                     workflow_name='TestWorkflow',
                                                                     workflow_version='1.0',
                                                                     task_list='unit_test_a')
        if wf_result:
            run_id = wf_result['runId']
            while True:
                time.sleep(5)
                wf_result = LiveSwfWorkflowTest.swf.describe_workflow_execution(domain='example', run_id=run_id,
                                                                                workflow_id='unit_test_a')
                if wf_result['executionInfo'].get('closeStatus') is not None:
                    break
            if wf_result['executionInfo'].get('closeStatus') != 'COMPLETED':
                self.fail('Workflow {} failed'.format(wf_result['executionInfo']['execution']['runId']))
        else:
            self.fail('Error when launching workflow')

    def test_simple_workflow_in_class(self):
        # TODO Test output of workflow, not just the status
        wf_result = LiveSwfWorkflowTest.swf.start_workflow_execution(domain='example', workflow_id='unit_test_e',
                                                                     workflow_name='TestWorkflow',
                                                                     workflow_version='1.0',
                                                                     task_list='unit_test_e')
        if wf_result:
            run_id = wf_result['runId']
            while True:
                time.sleep(5)
                wf_result = LiveSwfWorkflowTest.swf.describe_workflow_execution(domain='example', run_id=run_id,
                                                                                workflow_id='unit_test_e')
                if wf_result['executionInfo'].get('closeStatus') is not None:
                    break
            if wf_result['executionInfo'].get('closeStatus') != 'COMPLETED':
                self.fail('Workflow {} failed'.format(wf_result['executionInfo']['execution']['runId']))
        else:
            self.fail('Error when launching workflow')

    def test_workflow_with_timer(self):
        # TODO Test output of workflow, not just the status
        wf_result = LiveSwfWorkflowTest.swf.start_workflow_execution(domain='example', workflow_id='unit_test_b',
                                                                     workflow_name='TestWorkflow',
                                                                     workflow_version='1.0',
                                                                     task_list='unit_test_b')
        if wf_result:
            run_id = wf_result['runId']
            while True:
                time.sleep(5)
                wf_result = LiveSwfWorkflowTest.swf.describe_workflow_execution(domain='example', run_id=run_id,
                                                                                workflow_id='unit_test_b')
                if wf_result['executionInfo'].get('closeStatus') is not None:
                    break
            if wf_result['executionInfo'].get('closeStatus') != 'COMPLETED':
                self.fail('Workflow {} failed'.format(wf_result['executionInfo']['execution']['runId']))
        else:
            self.fail('Error when launching workflow')

    def test_workflow_with_cwf(self):
        # TODO Test output of workflow, not just the status
        wf_result = LiveSwfWorkflowTest.swf.start_workflow_execution(domain='example', workflow_id='unit_test_c',
                                                                     workflow_name='TestWorkflow',
                                                                     workflow_version='1.0',
                                                                     task_list='unit_test_c')
        if wf_result:
            run_id = wf_result['runId']
            while True:
                time.sleep(5)
                wf_result = LiveSwfWorkflowTest.swf.describe_workflow_execution(domain='example', run_id=run_id,
                                                                                workflow_id='unit_test_c')
                if wf_result['executionInfo'].get('closeStatus') is not None:
                    break
            if wf_result['executionInfo'].get('closeStatus') != 'COMPLETED':
                self.fail('Workflow {} failed'.format(wf_result['executionInfo']['execution']['runId']))
        else:
            self.fail('Error when launching workflow')

    def test_workflow_with_cache(self):
        # TODO Test output of workflow, not just the status
        wf_result = LiveSwfWorkflowTest.swf.start_workflow_execution(domain='example', workflow_id='unit_test_d',
                                                                     workflow_name='TestWorkflow',
                                                                     workflow_version='1.0',
                                                                     task_list='unit_test_d')
        if wf_result:
            run_id = wf_result['runId']
            while True:
                time.sleep(5)
                wf_result = LiveSwfWorkflowTest.swf.describe_workflow_execution(domain='example', run_id=run_id,
                                                                                workflow_id='unit_test_d')
                if wf_result['executionInfo'].get('closeStatus') is not None:
                    break
            if wf_result['executionInfo'].get('closeStatus') != 'COMPLETED':
                self.fail('Workflow {} failed'.format(wf_result['executionInfo']['execution']['runId']))
        else:
            self.fail('Error when launching workflow')

    def test_workflow_with_user_markers(self):
        wf_result = LiveSwfWorkflowTest.swf.start_workflow_execution(domain='example', workflow_id='unit_test_f',
                                                                     workflow_name='TestWorkflow',
                                                                     workflow_version='1.0',
                                                                     task_list='unit_test_f')
        if wf_result:
            run_id = wf_result['runId']
            while True:
                time.sleep(5)
                wf_result = LiveSwfWorkflowTest.swf.describe_workflow_execution(domain='example', run_id=run_id,
                                                                                workflow_id='unit_test_f')
                if wf_result['executionInfo'].get('closeStatus') is not None:
                    break
            if wf_result['executionInfo'].get('closeStatus') != 'COMPLETED':
                self.fail('Workflow {} failed'.format(wf_result['executionInfo']['execution']['runId']))
        else:
            self.fail('Error when launching workflow')


class LocalWorkflowTest(unittest.TestCase):

    def test_simple_workflow(self):
        decider = LocalDecisionWorker(decision_function=decider_a)
        result = decider.start()
        self.assertEquals(45, result)

    def test_workflow_with_timer(self):
        decider = LocalDecisionWorker(decision_function=decider_b)
        start_time = time.time()
        decider.start()
        end_time = time.time()
        self.assertAlmostEqual(end_time-start_time, 10, delta=2)

    def test_workflow_with_cwf(self):
        decider = LocalDecisionWorker(decision_function=decider_c)
        result = decider.start()
        self.assertEquals(70, result)
