from dpdispatcher.machine import Machine
import os, sys, json, glob, shutil, uuid, time
import unittest
from unittest.mock import MagicMock, patch, PropertyMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
__package__ = 'tests'
# from .context import LocalSession
# from .context import LocalContext
# from dpdispatcher.local_context import LocalContext
from .context import LocalContext
from .context import PBS
from .context import Slurm
from .context import JobStatus
from .context import Submission, Job, Task, Resources
# from .context import BaseDatabase
class SampleClass(object):
    @classmethod
    def get_sample_resources(cls):
        
        resources = Resources(number_node=1,
            cpu_per_node=4, 
            gpu_per_node=1,
            queue_name="T4_4_15",
            group_size=2,
            custom_flags=[],
            strategy={'if_cuda_multi_devices': False},
            para_deg=1,
            module_unload_list=[],
            module_list=[],
            source_list=[],
            envs={}
        )
        return resources
    
    @classmethod
    def get_sample_resources_dict(cls):
        resources_dict={'number_node': 1, 
            'cpu_per_node':4, 
            'gpu_per_node':1, 
            'queue_name':'T4_4_15', 
            'group_size':2,
            'custom_flags':[],
            'strategy':{'if_cuda_multi_devices': False}, 
            'para_deg':1,
            'module_unload_list':[],
            'module_list':[],
            'source_list':[],
            'envs':{},
            'kwargs': {}
        }
        return resources_dict

    @classmethod
    def get_sample_machine_dict(cls):
        machine_dict = {
            "batch_type": "Shell",
            "context_type": "LocalContext",
            "local_root": "./test_shell_trival_dir",
            "remote_root": "./tmp_shell_trival_dir"
        }
        return machine_dict
    
    @classmethod
    def get_sample_machine(cls):
        machine_dict = cls.get_sample_machine_dict()
        machine = Machine.load_from_dict(machine_dict)
        return machine

    @classmethod
    def get_sample_task(cls):
        task = Task(command='lmp -i input.lammps', task_work_path='bct-1/', forward_files=['conf.lmp', 'input.lammps'], backward_files=['log.lammps'], outlog='log', errlog='err')
        return task

    @classmethod
    def get_sample_task_dict(cls):
        task_dict = {'command': 'lmp -i input.lammps', 'task_work_path': 'bct-1/', 'forward_files': ['conf.lmp', 'input.lammps'] , 'backward_files': ['log.lammps'], 'outlog': 'log', 'errlog':'err'}
        return task_dict

    @classmethod
    def get_sample_task_list(cls):
        task1 = Task(command='lmp -i input.lammps', 
            task_work_path='bct-1/', forward_files=['conf.lmp', 'input.lammps'], backward_files=['log.lammps'])
        task2 = Task(command='lmp -i input.lammps', 
            task_work_path='bct-2/', forward_files=['conf.lmp', 'input.lammps'], backward_files=['log.lammps'])
        task3 = Task(command='lmp -i input.lammps', 
            task_work_path='bct-3/', forward_files=['conf.lmp', 'input.lammps'], backward_files=['log.lammps'])
        task4 = Task(command='lmp -i input.lammps', task_work_path='bct-4/', 
            forward_files=['conf.lmp', 'input.lammps'], backward_files=['log.lammps'])
        task_list = [task1, task2, task3, task4]
        return task_list

    @classmethod
    def get_sample_empty_submission(cls):
        resources = cls.get_sample_resources()
        # print(task_list)
        empty_submission = Submission(work_base='0_md/',
            machine=None,
            resources=resources, 
            forward_common_files=['graph.pb'], 
            backward_common_files=[])
        # print('SampleClass.get_sample_empty_submission:', empty_submission)
        # print('SampleClass.get_sample_empty_submission.belonging_tasks:', empty_submission.belonging_tasks)
        return empty_submission 
    
    @classmethod
    def get_sample_submission(cls):
        submission = cls.get_sample_empty_submission()
        task_list = cls.get_sample_task_list()
        submission.register_task_list(task_list)
        submission.generate_jobs()
        machine = cls.get_sample_machine()
        submission.bind_machine(machine=machine)
        return submission

    @classmethod
    def get_sample_submission_dict(cls):
        submission = cls.get_sample_submission()
        submission_dict = submission.serialize()
        return submission_dict
    
    @classmethod
    def get_sample_job(cls):
        Submission = cls.get_sample_submission()
        job = Submission.belonging_jobs[0]
        return job
    
    @classmethod
    def get_sample_job_dict(cls):
        job = cls.get_sample_job()
        job_dict = job.serialize()
        return job_dict
    
    @classmethod
    def get_sample_pbs_local_context(cls):
        # local_session = LocalSession({'work_path':'test_work_path/'})
        local_context = LocalContext(local_root='test_pbs_dir/',
            remote_root='tmp_pbs_dir/')
        pbs = PBS(context=local_context)
        return pbs

    @classmethod
    def get_sample_slurm_local_context(cls):
        # local_session = LocalSession({'work_path':'test_work_path/'})
        local_context = LocalContext(local_root='test_slurm_dir/',
            remote_root='tmp_slurm_dir/')
        slurm = Slurm(context=local_context)
        return slurm

    @classmethod
    def get_sample_shell_trival_submission(cls):
        machine = Machine(
            batch_type='Shell',
            context_type='LocalContext',
            local_root='./test_shell_trival_dir',
            remote_root='./tmp_shell_trival_dir'
        )

        resources = Resources(
            number_node=1,
            cpu_per_node=4,
            gpu_per_node=0,
            queue_name='CPU',
            group_size=2
        )

        task1 = Task(command='cat example.txt && sleep 0.5', task_work_path='dir1/', forward_files=['example.txt'], backward_files=['out.txt'], outlog='out.txt')
        task2 = Task(command='cat example.txt && sleep 0.5', task_work_path='dir2/', forward_files=['example.txt'], backward_files=['out.txt'], outlog='out.txt')
        task3 = Task(command='cat example.txt && sleep 0.5', task_work_path='dir3/', forward_files=['example.txt'], backward_files=['out.txt'], outlog='out.txt')
        task4 = Task(command='cat example.txt && sleep 0.5', task_work_path='dir4/', forward_files=['example.txt'], backward_files=['out.txt'], outlog='out.txt')
        task_list = [task1, task2, task3, task4]

        submission = Submission(work_base='parent_dir/',
            machine=machine,
            resources=resources,
            forward_common_files=['graph.pb'],
            backward_common_files=[],
            task_list=task_list
        )
        return submission
