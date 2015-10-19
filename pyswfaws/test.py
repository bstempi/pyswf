from multiprocessing import Process
from nose.plugins.attrib import attr
from decisionworker import DecisionWorker
from activityworker import ActivityWorker
from models import SwfDecisionContext

import unittest
import time

import boto
import serializers
import datastores


class TestDecider(DecisionWorker):
    """
    A simple decider to use in the test cases
    """
    class Meta:
        swf_domain = 'example'
        swf_task_list = 'some_list'
        serializer = serializers.JsonSerializer()
        data_store = datastores.SwfDataStore()

    def handle(self, **kwargs):
        worker_a = TestActivityWorkerA()
        worker_b = TestActivityWorkerB()

        # Should return a promise containing the value 36
        result_a = worker_a.handle_task(6)
        # Should return a promise containing the value 3
        result_b = worker_b.handle_task(6)

        return result_a.result + result_b.result


class TestDeciderWithCachedResult(DecisionWorker):
    class Meta:
        swf_domain = 'example'
        swf_task_list = 'some_list'
        serializer = serializers.JsonSerializer()
        data_store = datastores.SwfDataStore()

    def handle(self, **kwargs):
        worker_a = TestActivityWorkerA()
        worker_b = TestActivityWorkerB()

        # Should return a promise containing the value 36
        result_a = worker_a.handle_task(6)
        # Should return a promise containing the value 3
        result_b = worker_b.handle_task(6)

        some_cached_result = self.some_long_running_call()

        return result_a.result + result_b.result + some_cached_result

    @DecisionWorker.cached
    def some_long_running_call(self):
        return 1


class TestActivityWorkerA(ActivityWorker):
    """
    A simple activity worker to use in test cases
    """
    class Meta:
        swf_domain = 'example'
        swf_task_list = 'activity_a_test'
        activity_type = 'TestActivityA'
        activity_version = '1.0'
        serializer = serializers.JsonSerializer()
        data_store = datastores.SwfDataStore()

    @ActivityWorker.activity_task
    def handle_task(self, a):
        return a * a


class TestActivityWorkerB(ActivityWorker):
    """
    A simple activity worker to use in test cases
    """
    class Meta:
        swf_domain = 'example'
        swf_task_list = 'activity_b_test'
        activity_type = 'TestActivityB'
        activity_version = '1.0'
        serializer = serializers.JsonSerializer()
        data_store = datastores.SwfDataStore()

    @ActivityWorker.activity_task
    def handle_task(self, b):
        return b / 2


class CoreSerialTest(unittest.TestCase):
    """
    Tests to make sure that serial execution works
    """
    def test_core_serial_happy_path(self):
        SwfDecisionContext.mode = SwfDecisionContext.SerialLocal

        # Test to make sure the workers function correctly
        test_worker_a = TestActivityWorkerA()
        result_a = test_worker_a.handle_task(6)
        self.assertEqual(True, result_a.is_ready)
        self.assertEqual(36, result_a.result)

        test_worker_b = TestActivityWorkerB()
        result_b = test_worker_b.handle_task(6)
        self.assertEqual(True, result_b.is_ready)
        self.assertEqual(3, result_b.result)

        test_decider = TestDecider(SwfDecisionContext.SerialLocal)
        final_result = test_decider.handle()
        self.assertEqual(final_result, 39)

    def test_core_with_marker_happy_path(self):
        SwfDecisionContext.mode = SwfDecisionContext.SerialLocal

        # Test to make sure the workers function correctly
        test_worker_a = TestActivityWorkerA()
        result_a = test_worker_a.handle_task(6)
        self.assertEqual(True, result_a.is_ready)
        self.assertEqual(36, result_a.result)

        test_worker_b = TestActivityWorkerB()
        result_b = test_worker_b.handle_task(6)
        self.assertEqual(True, result_b.is_ready)
        self.assertEqual(3, result_b.result)

        test_decider = TestDeciderWithCachedResult(SwfDecisionContext.SerialLocal)
        final_result = test_decider.handle()
        self.assertEqual(final_result, 40)


class CoreDistributedTest(unittest.TestCase):
    """
    Test to make sure that the core parts of distributed exeuction work
    """
    def test_core_distributed_first_run_happy_path(self):
        SwfDecisionContext.mode = SwfDecisionContext.Distributed
        test_decider = TestDecider(SwfDecisionContext.SerialLocal)
        try:
            test_decider.handle()
        except SystemExit:
            pass

        self.assertEqual(2, len(SwfDecisionContext.decisions._data))
        first_decision = SwfDecisionContext.decisions._data[0]
        second_decision = SwfDecisionContext.decisions._data[1]

        self.assertEqual(str(1), first_decision[
            'scheduleActivityTaskDecisionAttributes']['activityId'])
        self.assertEqual(TestActivityWorkerA.Meta.swf_task_list, first_decision[
            'scheduleActivityTaskDecisionAttributes']['taskList']['name'])
        self.assertEqual(TestActivityWorkerA.Meta.activity_type, first_decision[
            'scheduleActivityTaskDecisionAttributes']['activityType']['name'])
        self.assertEqual(TestActivityWorkerA.Meta.activity_version, first_decision[
            'scheduleActivityTaskDecisionAttributes']['activityType']['version'])
        self.assertEqual('[[6], {}]', first_decision['scheduleActivityTaskDecisionAttributes']['input'])

        self.assertEqual(str(2), second_decision[
            'scheduleActivityTaskDecisionAttributes']['activityId'])
        self.assertEqual(TestActivityWorkerB.Meta.swf_task_list, second_decision[
            'scheduleActivityTaskDecisionAttributes']['taskList']['name'])
        self.assertEqual(TestActivityWorkerB.Meta.activity_type, second_decision[
            'scheduleActivityTaskDecisionAttributes']['activityType']['name'])
        self.assertEqual(TestActivityWorkerB.Meta.activity_version, second_decision[
            'scheduleActivityTaskDecisionAttributes']['activityType']['version'])
        self.assertEqual('[[6], {}]', second_decision['scheduleActivityTaskDecisionAttributes']['input'])


@attr('integration')
class LiveSwfWorkflowTest(unittest.TestCase):
    """
    Test that runs a workflow in SWF
    """

    # TODO Test output of workflow, not just the status

    @classmethod
    def setUpClass(cls):
        LiveSwfWorkflowTest.swf = boto.connect_swf()

        LiveSwfWorkflowTest.decision_worker_simple = TestDecider(mode=SwfDecisionContext.Distributed)
        LiveSwfWorkflowTest.decision_worker_cached = TestDeciderWithCachedResult(mode=SwfDecisionContext.Distributed)
        LiveSwfWorkflowTest.activity_worker_a = TestActivityWorkerA()
        LiveSwfWorkflowTest.activity_worker_b = TestActivityWorkerB()

        LiveSwfWorkflowTest.decider_simple_process = Process(target=LiveSwfWorkflowTest.decision_worker_simple.start)
        LiveSwfWorkflowTest.decider_cached_process = Process(target=LiveSwfWorkflowTest.decision_worker_cached.start)
        LiveSwfWorkflowTest.activity_worker_a_process = Process(target=LiveSwfWorkflowTest.activity_worker_a.start)
        LiveSwfWorkflowTest.activity_worker_b_process = Process(target=LiveSwfWorkflowTest.activity_worker_b.start)

        LiveSwfWorkflowTest.decider_simple_process.start()
        LiveSwfWorkflowTest.decider_cached_process.start()
        LiveSwfWorkflowTest.activity_worker_a_process.start()
        LiveSwfWorkflowTest.activity_worker_b_process.start()
        time.sleep(5)

    @classmethod
    def tearDownClass(cls):
        LiveSwfWorkflowTest.decider_simple_process.terminate()
        LiveSwfWorkflowTest.decider_cached_process.terminate()
        LiveSwfWorkflowTest.activity_worker_a_process.terminate()
        LiveSwfWorkflowTest.activity_worker_b_process.terminate()

    def test_live_workflow_simple_case(self):
        wf_result = LiveSwfWorkflowTest.swf.start_workflow_execution(domain='example', workflow_id='unit_test',
                                                                     workflow_name='TestWorkflow',
                                                                     workflow_version='1.0')
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

    def test_live_workflow_contains_cache(self):
        wf_result = LiveSwfWorkflowTest.swf.start_workflow_execution(domain='example', workflow_id='unit_test',
                                                                     workflow_name='TestWorkflow',
                                                                     workflow_version='1.0')

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
