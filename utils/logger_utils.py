import datetime
import logging
import os
import re
import time
import zipfile
from multiprocessing import Process

import schedule
from logging.handlers import RotatingFileHandler

from utils.mongo_util import MongoTaskResult
from utils.redis_util import RedisUtil

log_dir_prefix = "/home/oasis-log/"
running_task_key = os.getenv("RUNNING_TASK_KEY")

if os.path.exists("/.dockerenv"):
    redisUtils = RedisUtil()
    mongo = MongoTaskResult()


    def get_task_id():
        task_id = None
        print("running_task_key:{}".format(running_task_key))
        if not running_task_key:
            return task_id
        task_id = redisUtils.get(running_task_key)
        return task_id


    def get_user_email(task_id):
        if not task_id:
            return None
        # 从redis里面查询 user_email
        user_email = redisUtils.get(running_task_key + task_id)
        if user_email:
            return user_email
        # 从mongo 的job表查询 usr_id
        user_id = mongo.find_user_id_from_job(task_id)
        # 从 user 表查询 用户名
        user = mongo.find_user_info(user_id)
        # 写入redis
        if not user:
            return None
        redisUtils.set(running_task_key + task_id, user["email"])
        return user["email"]


def singleton(cls):
    instances = {}

    def _singleton(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return _singleton


class LogFilter(logging.Filter):
    """
    This is a filter which injects contextual information into the log.
    """

    def filter(self, record):
        record.task_id = None
        record.user_email = None
        if os.path.exists("/.dockerenv"):
            task_id = get_task_id()
            record.task_id = task_id
            record.user_email = get_user_email(task_id)
        zip_files()
        return True


@singleton
class LogWrapper:
    def __init__(self, server_name="sdg-log"):
        self.logger = logging.getLogger(__name__)
        self.log_dir_prefix = "/home/oasis-log/"
        self.server_name = server_name
        global glob_server_name
        glob_server_name = server_name

        # init logger
        self.logger.setLevel(level=logging.INFO)
        log_dir_prefix = os.path.abspath(os.path.join(self.log_dir_prefix, self.server_name))
        if not os.path.exists(log_dir_prefix):
            os.makedirs(log_dir_prefix)
        # handler = RotatingFileHandler(
        #     filename="{}/{}_log.txt".format(log_dir_prefix, time.strftime("%Y_%m_%d_%H", time.localtime())),
        #     maxBytes=10 * 1024 * 1024,
        #     backupCount=100,
        #     encoding='utf-8')

        # init_time = (datetime.datetime.now()).strftime('%Y-%m-%d-%H')
        # init_name = init_time + ".log"

        formatter = logging.Formatter(
            '%(asctime)s - %(user_email)s - %(task_id)s - %(filename)s - %(lineno)s - %(levelname)s - %(message)s')

        init_name = "sdg.log"
        # 每隔1个小时，保存一个日志文件，备份文件为30 * 24个
        handler = logging.handlers.TimedRotatingFileHandler("{}/{}".format(log_dir_prefix, init_name), when="H",
                                                            interval=1,
                                                            backupCount=30 * 24)

        def namer(filename):
            return filename.replace("/" + init_name + ".", "/")

        handler.namer = namer
        handler.suffix = "%Y-%m-%d-%H.log"
        handler.extMatch = re.compile(r"^\d{4}-\d{2}-\d{2}-\d{2}.zip$")
        handler.setLevel(logging.INFO)

        if not os.path.exists("/.dockerenv"):
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.addFilter(LogFilter())

        # schedule zip log
        t1 = Process(target=schedule_task)
        t1.start()

    def getlogger(self):
        return self.logger


def zip_files():
    time_name = (datetime.datetime.now() - datetime.timedelta(hours=2)).strftime('%Y-%m-%d-%H')
    file_name = time_name + ".log"
    file_path = os.path.join(log_dir_prefix, glob_server_name, file_name)
    if not os.path.exists(file_path):
        return

    zip_name = os.path.join(log_dir_prefix, glob_server_name, time_name + ".zip")
    zip = zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED)
    zip.write(file_path)
    zip.close()
    os.remove(file_path)


def schedule_task():
    schedule.every(1).hours.do(zip_files)
    while True:
        schedule.run_pending()
        time.sleep(1)