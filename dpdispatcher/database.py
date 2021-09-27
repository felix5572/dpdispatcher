#%%
import time,random,uuid,json,copy
import os
import sqlite3
from dargs.dargs import Argument
from hashlib import sha1
from abc import ABC, abstractmethod
from dpdispatcher import dlog

#%%
class BaseDatabase(object):
    subclasses_dict = {}
    default_database_settings = {
        'database_type': 'jsondatabase',
        'database_location': 'remote'
    }

    def __new__(cls, *args, **kwargs):
        if kwargs == {}:
            kwargs = cls.default_database_settings
        if cls is BaseDatabase:
            subcls = cls.subclasses_dict[kwargs['database_type']]
            instance = subcls.__new__(subcls, *args, **kwargs)
        else:
            instance = object.__new__(cls)
        return instance

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.subclasses_dict[cls.__name__] = cls
        cls.subclasses_dict[cls.__name__.lower()] = cls

    def __init__(self, 
        database_type=None,
        database_location={},
    ):
        if database_type is not None:
            print(f"debug database type {database_type}")
            assert database_type.lower() == self.__class__.database_type
        self.database_type = database_type
        self.database_location = database_location

        self.machine = None
        self.setup_database()

    def setup_database(self):
        pass

    def serialize(self):
        database_dict = {}
        database_dict['database_type'] = self.database_type
        database_dict['database_location'] = self.database_location
        return database_dict

    def bind_machine(self, machine):
        self.machine = machine

    @abstractmethod
    def save_to_database(self, submission):
        pass

    @abstractmethod
    def update_in_database(self, submission):
        pass

    @abstractmethod
    def recover_submission_from_database(self):
        submission = None
        return submission

    @abstractmethod
    def update_submission_state(self, submission):
        pass
#%%

class SQLiteDatabase(BaseDatabase):
    database_type = 'sqlitedatabase'

    def setup_connection(self):
        self.con = sqlite3.connect('dpdispatcher.db')
        self.cur = self.con.cursor()

    def create_database(self):
        create_job_table_sql = '''CREATE TABLE job_table (
            job_hash text NOT NULL PRIMARY KEY,
            job_task_list json,
            resources json,
            job_state integer,
            job_id text,
            fail_count integer,
            submission_hash text
        );'''

        create_submission_table_sql = '''CREATE TABLE submission_table (
            submission_hash text,
            machine_dict json,
            work_base text,
            resources json,
            forward_common_files json,
            backward_common_files json,
            belonging_jobs_hash json
        );'''

        self.cur.execute(create_job_table_sql)
        self.cur.execute(create_submission_table_sql)
        self.con.commit()

    def save_submission_to_database(self, submission):
        submission_dict = submission.serialize()
        submission_hash = submission.submission_hash
        machine_dict = submission_dict['machine_dict']
        work_base = submission_dict['work_base']
        resources = submission_dict['resources']
        forward_common_files = submission_dict['forward_common_files']
        backward_common_files = submission_dict['backward_common_files']
        belonging_jobs_hash = list(submission_dict['belonging_jobs'].keys())

        sql = f"""INSERT INTO submission_table 
            (submission_hash, machine_dict, work_base, resources, 
            forward_common_files, backward_common_files, belonging_jobs_hash)
            VALUES ('{submission_hash}', 
                '{machine_dict}', 
                '{work_base}', 
                '{resources}', 
                '{forward_common_files}', 
                '{backward_common_files}',
                '{belonging_jobs_hash}'); """
        self.cur.execute(sql)
        self.con.commit()
        for job in submission.belonging_jobs:
            self.save_job_to_database(job)

    def save_job_to_database(self, job):
        job_dict = job.serialize()
        job_hash = job.job_hash
        job_task_list = job_dict['job_task_list']
        resources = job_dict['resources']
        job_state = job_dict['job_state']
        job_id = job_dict['job_id']
        fail_count = job_dict['fail_count']
        submission_hash = job.submission.submission_hash

        sql = f"""INSERT INTO job_table
            (job_hash, job_task_list, resources, 
            job_state, job_id, fail_count, submission_hash)
            VALUES ('{job_hash}', 
                '{job_task_list}', 
                '{resources}', 
                '{job_state}', 
                '{job_id}', 
                '{fail_count}',
                '{submission_hash}'); """
        self.cur.execute(sql)
        self.con.commit()

    def get_job_from_database(self, job_hash):
        pass

    def update_submission_jobs_in_database(self, submission):
        for job in submission.belonging_jobs:
            self.update_job_state_in_database(job)

    def update_job_in_database(self, job):
        job_hash = job.job_hash
        job_state = job.job_state
        job_id = job.job_id
        fail_count = job.fail_count
        sql = f"""UPDATE job_table
            SET job_state = {job_state},
            job_id = '{job_id}',
            fail_count = {fail_count}
            WHERE job_hash = '{job_hash}';
        """
        self.cur.execute(sql)
        self.con.commit()

    def save_job_to_database(self, job):
        job_dict = job.serialize()
        job_hash = job.job_hash
        job_task_list = job_dict['job_task_list']
        resources = job_dict['resources']
        job_state = job_dict['job_state']
        job_id = job_dict['job_id']
        fail_count = job_dict['fail_count']
        submission_hash = job.submission.submission_hash

        sql = f"""INSERT INTO job_table
            (job_hash, job_task_list, resources, 
            job_state, job_id, fail_count)
            VALUES ('{job_hash}', 
                '{job_task_list}', 
                '{resources}', 
                '{job_state}', 
                '{job_id}',
                '{fail_count}',
                '{submission_hash}');"""

        self.cur.execute(sql)
        self.con.commit()

    # def get_submission

    def recover_submission_from_database(self, submission):
        submission_hash = submission.submission_hash

        return submission

    def update_submission_state(self, submission):
        pass

class JSONDatabase(BaseDatabase):
    database_type = 'jsondatabase'
    def save_submission_to_database(self, submission):
        write_str = json.dumps(submission.serialize(), indent=4, default=str)
        submission_file_name = submission.submission_hash + '.json'
        submission_file_path = os.path.join(self.machine.context.subm_remote_root, submission_file_name)
        self.machine.context.write_file(submission_file_name, write_str=write_str)

    def update_submission_in_database(self, submission):
        self.save_submission_to_database(submission)

    def recover_submission_from_database(self, submission):
        submission_hash = submission.submission_hash
        submission_file_name = "{submission_hash}.json".format(submission_hash=submission_hash)
        if_recover = self.machine.context.check_file_exists(submission_file_name)
        submission_dict = {}
        if if_recover :
            submission_dict_str = submission.machine.context.read_file(fname=submission_file_name)
            submission_dict = json.loads(submission_dict_str)
            recovered_submission = submission.__class__.deserialize(submission_dict=submission_dict)
            print('debug1989', submission.machine.serialize(), recovered_submission.machine.serialize())
            print('debug1990', submission.submission_hash, recovered_submission.submission_hash)
            print('debug1991', submission == recovered_submission)
            recovered_submission.bind_machine(machine=submission.machine)

            if submission == recovered_submission:
                submission.belonging_jobs = recovered_submission.belonging_jobs
                dlog.info(f"Find old submission; recover from json; "
                    f"submission.submission_hash:{submission.submission_hash}; "
                    f"machine.context.remote_root:{submission.machine.context.remote_root}; "
                    f"submission.work_base:{submission.work_base};")
            else:
                print(submission.serialize())
                raise RuntimeError("Recover failed Please check the database.")


# %%
# print(BaseDatabase.subclasses_dict)
# j = JSONDatabase()
# print(BaseDatabase.subclasses_dict)
# %%
# %%
