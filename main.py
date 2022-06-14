import datetime
import time

from utils.logger_utils import LogWrapper
logging = LogWrapper("sdg-test").getlogger()
logging2 = LogWrapper("sdg-test").getlogger()
logging3 = LogWrapper("sdg-test").getlogger()

i = 0
if __name__ == '__main__':
    while True:
        init_time = (datetime.datetime.now()).strftime('%Y-%m-%d-%H-%M-%S')
        i = i + 1
        logging.info("{}-test1.................{}".format(init_time, i))
        i = i + 1
        logging2.info("{}-test2.................{}".format(init_time,  i))
        i = i + 1
        logging3.info("{}-test3.................{}".format(init_time,  i))
        time.sleep(0.1)
