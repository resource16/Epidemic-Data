import logging
import os
import re
from logging.handlers import TimedRotatingFileHandler


def make_log(name, logger_name):

    # 创建日志文件夹
    logPath = "log/"

    if not os.path.exists(logPath):
        os.makedirs(logPath)

    # 设置日志文件的文件名
    infoName = "%s.log" % name
    infoFile = logPath + infoName

    # 创建及配置logger
    logger = logging.getLogger(logger_name)

    # 级别：CRITICAL > ERROR > WARNING > INFO > DEBUG，默认级别为 WARNING
    logger.setLevel(logging.INFO)

    
    # 设置formatter，日志的输出格式
    fmt = '%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'

    # 每行日志的前缀设置
    format_str = logging.Formatter(fmt)

    # 往屏幕上输出
    sh = logging.StreamHandler()

    # 设置屏幕上显示的格式
    sh.setFormatter(format_str)

    #往文件里写入#指定间隔时间自动生成文件的处理器
    #实例化TimedRotatingFileHandler
    #interval是时间间隔，backupCount是备份文件的个数，如果超过这个个数，就会自动删除，when是间隔的时间单位，单位有以下几种：
    # S 秒
    # M 分
    # H 小时、
    # D 天、
    # W 每星期（interval==0时代表星期一）
    # midnight 每天凌晨
    th = TimedRotatingFileHandler(infoFile, when='midnight', interval=1, backupCount=7, encoding='utf-8')
    
    #设置 切分后日志文件名的时间格式 默认 filename+"." + suffix 如果需要更改需要改logging 源码
    th.suffix = "%Y%m%d.log"

    # 正则匹配
    th.extMatch = re.compile(r"^\d{4}\d{2}\d{2}.log$")

    # 写入日志的格式
    th.setFormatter(format_str)

    # 为该logger对象添加一个handler对象
    logger.addHandler(sh)
    logger.addHandler(th)

    return logger
