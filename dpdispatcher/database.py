import time,random,uuid,json,copy
import os
from dargs.dargs import Argument
from dpdispatcher.JobStatus import JobStatus
from dpdispatcher import dlog, submission
from hashlib import sha1
from abc import ABC, abstractmethod

#%%
class BaseDatabase(ABC):
    def __init__subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.subclasses_dict[cls.__name__] = cls

    def __init__(self, db_file_dir):
        self.db_file_dir = db_file_dir
    
    @abstractmethod
    def save_to_db(self, submission):
        pass

    @abstractmethod
    def recover_submission_from_db(self):
        submission = None
        return submission

    @abstractmethod
    def update_submission_state(self, submission):
        pass


class SQLiteDB(BaseDatabase):
    def __init__(self, db_file_dir):
        self.db_file_dir = db_file_dir
    
    def save_to_db(self, submission):
        pass

    def recover_submission_from_db(self):
        submission = None
        return submission

    def update_submission_state(self, submission):
        pass

class JSONDB(BaseDatabase):
    def __init__(self, db_file_dir):
        self.db_file_dir = db_file_dir

    def bind_machine(self, machine):
        self.machine = machine

    def save_submission_to_db(self, submission):
        write_str = json.dumps(submission.serialize(), indent=4, default=str)
        submission_file_name = submission.submission_hash + '.json'
        submission_file_path = os.path.join(self.machine.context.remote_root, submission_file_name)
        self.machine.context.write_file(submission_file_path, write_str=write_str)

    def recover_submission_from_db(self):
        submission_file_name = self.machine.context.submission.submission_hash + '.json'
        if_recover = self.machine.check_if_recover(submission_file_name)
        if if_recover:
            submission = self.machine.recover_submission()
        return submission
