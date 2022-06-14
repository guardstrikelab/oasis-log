import os

from redis import Redis


class RedisUtil:
    def __init__(self):
        # self.conf = get_conf()
        # self.host = self.conf['DB_REDIS']['REDIS_HOST']
        # self.port = self.conf['DB_REDIS']['REDIS_PORT']
        # self.password = self.conf['DB_REDIS']['REDIS_PASSWORD']
        # self.db = self.conf['DB_REDIS']['REDIS_DB']
        self.host = os.getenv("REDIS_HOST")
        self.port = os.getenv("REDIS_PORT")
        self.password = os.getenv("REDIS_PASSWORD")
        self.db = os.getenv("REDIS_DB")

    def __session(self):
        while True:
            try:
                session = Redis(host=str(self.host),
                                port=int(self.port),
                                password=str(self.password),
                                db=int(self.db),
                                decode_responses=True)
                session.ping()
            except Exception as e:
                print('redis connection failed, try to re-connecting!')
                continue
            else:
                return session

    # def check_msg(self):
    #     msg_info = self.__session().rpop("job_task_info")

    def rpop(self, key):
        return self.__session().rpop(key)

    def get(self, key):
        return self.__session().get(key)

    def set(self, key, value):
        # string 类型设置过期时间5 分钟
        self.__session().set(key, value, ex=5 * 60)
