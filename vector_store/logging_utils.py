import logging
from logging.handlers import TimedRotatingFileHandler
import os

is_debug = False


class Logger(object):
    def __init__(self, log_level, logger_name, log_file_name=None):
        # 创建一个logger
        self.__logger = logging.getLogger(logger_name)

        # 指定日志的最低输出级别，默认为WARN级别
        self.__logger.setLevel(log_level)

        # 定义handler的输出格式
        formatter = logging.Formatter('[%(asctime)s]-[%(filename)s line:%(lineno)d]-%(levelname)s: %(message)s')

        if log_file_name:
            # 创建一个handler用于写入日志文件,设置输出格式, 给logger添加handler
            file_handler = TimedRotatingFileHandler(filename=log_file_name, encoding="utf-8", when="D", interval=1,
                                                    backupCount=7)
            # file_handler = TimedRotatingFileHandler(filename=log_file_name,encoding="utf-8",when="S", interval=5, backupCount=4)

            file_handler.setFormatter(formatter)
            self.__logger.addHandler(file_handler)

        # 创建一个handler用于输出控制台
        # console_handler = logging.StreamHandler()
        # console_handler.setFormatter(formatter)
        # 给logger添加handler
        # self.__logger.addHandler(console_handler)

    def get_log(self):
        return self.__logger


path = os.path.abspath(__file__)
dir = os.path.dirname(path)
log_file = os.path.join(dir, 'logs/log.txt')
os.makedirs(os.path.join(dir, 'logs'), exist_ok=True)

if is_debug:
    logger = Logger(log_level=logging.DEBUG, logger_name="test", log_file_name=log_file).get_log()
else:
    logger = Logger(log_level=logging.INFO, logger_name="test", log_file_name=log_file).get_log()

