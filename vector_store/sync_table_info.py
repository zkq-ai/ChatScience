# coding: utf-8
"""
功能：同步表信息至向量库

Author：zhuokeqiu
Date ：2024/06/07 9:47
"""

from db.get_db_info import run as get_db_info
from process import TableEnumeration
import traceback
import time
import schedule
from logging_utils import logger
from arg_helper import args

def job():
    logger.info("定时任务执行中...")
    table_info_file=f'data/tableinfo_{args.project_name}.json'
    try:
        get_db_info(table_info_file)
        table_enumeration=TableEnumeration()
        table_enumeration.init_table_enumeration(table_info_file)
        table_enumeration.init_table_columns(table_info_file)
        logger.info("定时任务执行执行完毕！")
    except:
        logger.error(traceback.format_exc())
        logger.error("定时任务执行执行失败！")




if __name__ == '__main__':

    #/home/zkq/.conda/envs/zkq_py310/bin/python
    job()
    # 每天的 10:30 执行一次任务
    # schedule.every().day.at(args.sync_time).do(job)

    # 每隔 10 秒钟执行一次任务
    # schedule.every(10).seconds.do(job)

    # while True:
    #     schedule.run_pending()
    #     time.sleep(30)