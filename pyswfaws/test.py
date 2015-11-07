from multiprocessing import Process
from nose.plugins.attrib import attr
from datastores import *
from serializers import *
from decorators import *
from activityworker import *

import unittest
import time

import boto


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

    # TODO Test output of workflow, not just the status

    @classmethod
    def setUpClass(cls):
        LiveSwfWorkflowTest.swf = boto.connect_swf()

        LiveSwfWorkflowTest.activity_task_a_runner = DistributedActivityWorker(activity_task_a)
        LiveSwfWorkflowTest.activity_task_a_process = Process(target=LiveSwfWorkflowTest.activity_task_a_runner.start)
        LiveSwfWorkflowTest.activity_task_a_process.start()
        time.sleep(15)

    @classmethod
    def tearDownClass(cls):
        LiveSwfWorkflowTest.activity_task_a_process.terminate()

    def test_stuff(self):
        pass
