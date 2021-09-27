import sys, os, time
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
__package__ = 'tests'
from .context import Submission, Job, Task, Resources
from .context import Shell
from .context import LocalContext
from .context import get_file_md5
from .context import Machine
from .context import JobStatus
from .sample_class import SampleClass

import unittest
import json, shutil

class TestShellRecover(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None

    def test_exit_on_submit(self):
        submission = SampleClass.get_sample_shell_trival_submission()
        submission_dict = submission.run_submission(exit_on_submit=True)
        for ii in submission_dict['belonging_jobs']:
            self.assertEqual(list(ii.values())[0]['job_state'], JobStatus.running)
        time.sleep(1)
        submission_dict = submission.run_submission()
        for ii in submission_dict['belonging_jobs']:
            self.assertEqual(list(ii.values())[0]['job_state'], JobStatus.finished)
            # print(ii.values()['job_state'])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree('tmp_shell_trival_dir/')

        