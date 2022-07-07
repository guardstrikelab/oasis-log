#!/usr/bin/env python
# coding: utf-8
import os
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field
from pymongo import MongoClient
from tenacity import retry, wait_fixed, stop_after_delay

RETRY_DELAY = 60 * 5
RETRY_INTERVAL = 3


class MongoSession:
    def __init__(self):
        # self.conf = get_conf()
        # self.uri = self.conf["DB_MONGO"]["MONGO_CONNECTION_STRING"]
        # self.db_name = self.conf["DB_MONGO"]["MONGO_DB_NAME"]
        self.uri = os.getenv("MONGO_CONNECTION_STRING")
        self.db_name = os.getenv("MONGO_DB_NAME")

        self.client = None
        self.__connect()
        self.session = self.client[str(self.db_name)]

    @retry(stop=stop_after_delay(RETRY_DELAY),
           wait=wait_fixed(RETRY_INTERVAL), reraise=True)
    def __connect(self):
        self.client = MongoClient(str(self.uri), uuidRepresentation="standard")
        self.client.server_info()

    def __disconnect(self):
        if self.client:
            self.client.close()


class MongoTaskResult:
    def __init__(self):
        self.session = MongoSession().session

    def update_result(self, task_id, job_id, running_result):
        self.update_task_result_to_db(task_id, job_id, running_result, field="result")

    def update_status(self, task_id, job_id, task_status):
        self.update_task_result_to_db(task_id, job_id, task_status, field="status")

    def update_task_result_to_db(self, task_id, job_id, res, field=None):
        """
        Update the task test result to the corresponding job and store it in the database
        Args:
            task_id: Id of the task of the test object
            job_id: Id of the job to which the task belongs
            field: Fields to be updated in the task
            res: Test result of the corresponding field
        Returns:
        """
        job = self.session.job.find_one({"id": job_id})
        for task in job["task_list"]:
            if task_id == str(task["id"]):
                task[field] = res
        result = self.session.job.update_one({'id': job_id}, {'$set': job})
        return result

    def check_task_status(self, job_id, task_id):
        job = self.session.job.find_one({"id": job_id})
        for task in job["task_list"]:
            if task_id == str(task["id"]):
                return task["status"]

    def find_user_id_from_job(self, task_id):
        job = self.session.job.find_one({"task_list.id": task_id})
        if job:
           return job["usr_id"]
        else:
            return None

    def find_user_info(self, usr_id):
        user = self.session.users.find_one({"id": usr_id})
        return user

class LogLevel(str, Enum):
    DEBUG = 'DEBUG'
    INFO = 'INFO'
    WARN = 'WARN'
    ERROR = 'ERROR'


class LogMsg(BaseModel):
    logger: str
    msg_dict: dict
    log_level: LogLevel
    updated: datetime = Field(default_factory=datetime.now)


class MongoLog(object):

    def __init__(self):
        self.log_collection = MongoSession().session["logs"]
        self.logger_name = 'sdg-listener'

    def debug_log(self, msg_dict):
        validated_logdict = LogMsg(logger=self.logger_name, msg_dict=msg_dict, log_level="DEBUG").dict()
        self.log_collection.insert_one(validated_logdict)

    def info_log(self, msg_dict):
        validated_logdict = LogMsg(logger=self.logger_name, msg_dict=msg_dict, log_level="INFO").dict()
        self.log_collection.insert_one(validated_logdict)

    def warn_log(self, msg_dict):
        validated_logdict = LogMsg(logger=self.logger_name, msg_dict=msg_dict, log_level="WARN").dict()
        self.log_collection.insert_one(validated_logdict)

    def error_log(self, msg_dict):
        validated_logdict = LogMsg(logger=self.logger_name, msg_dict=msg_dict, log_level="ERROR").dict()
        self.log_collection.insert_one(validated_logdict)
