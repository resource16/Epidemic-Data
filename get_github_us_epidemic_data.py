# -*- coding: utf-8 -*- 
# author: Ziyuan
# date: 2020-03-24


import sys
import os
import importlib
import json
import requests
import cx_Oracle as cx
import urllib
from urllib.request import Request, urlopen
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from loger import make_log
from datetime import datetime, date, timedelta

# 编码问题
importlib.reload(sys)
os.environ['NLS_LANG'] = 'Simplified Chinese_CHINA.ZHS16GBK'

# 主机断开连接问题
import socket
socket.setdefaulttimeout(20)  # 设置socket层的超时时间为20秒

logger = make_log('us_epidemic','github_epidemic')

# 代理
proxy='zzy:root123@192.168.66.199:80'
proxy_handler = {
    'http': 'http://' + proxy,
    'https': 'https://' + proxy
}


# 无代理
def get_data(url):
    firefox_headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}
    # response = urlopen(url=url, timeout=60)
    request =  Request(url, headers=firefox_headers)
    response = urlopen(request, timeout=30)
    # 字节转换字符串
    content = response.read().decode().replace(u'\xf1', u'').replace(u"'", u"")
    return content


# 有代理
def get_data_proxy(url):
    firefox_headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}
    request =  Request(url, headers=firefox_headers)
    # 创建一个ProxyHandler对象
    proxy_support=urllib.request.ProxyHandler(proxy_handler)
    # 创建一个opener对象
    opener = urllib.request.build_opener(proxy_support)
    # 给request装载opener
    urllib.request.install_opener(opener)
    response = urlopen(request)
    # 字节转换字符串
    content = response.read().decode().replace(u'\xf1', u'').replace(u"'", u"")
    return content

# 格式化时间
def fmt_date(date_str):
    try:
        format_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            format_date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            try:
                format_date = datetime.strptime(date_str, "%m/%d/%y %H:%M")
            except ValueError:
                try:
                    format_date = datetime.strptime(date_str, "%m/%d/%y %H:%M:%S")
                except ValueError:
                    format_date = date_str
    finally:
        format_date_str = format_date.strftime('%Y-%m-%d %H:%M:%S')
    return format_date_str


def parse_data():
    conn = cx.connect('flxuser/flxuser@192.168.158.219:1521/bfcecdw')
    cur = conn.cursor()
    logger.info(' --- 数据库连接 --- ')
    try:
        # 获取当前最晚同步次数
        s_sql = '''SELECT MAX(SYNC_NUMBER) FROM BASE_00070_NCOV_US'''
        cur.execute(s_sql)
        max_sn = cur.fetchone()[0]
        if max_sn == None:
            max_sn = 1
        else:
            max_sn += 1
        
        # 拼接url
        cur_date = (date.today() + timedelta(days = -1)).strftime('%m-%d-%Y')
        url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/%s.csv' % cur_date
        print(url)
        us_data = get_data(url)

        data_list = us_data.split('"')

        for i in range(len(data_list)):
            data = data_list[i].split(',')
            if 'US' in data:
                if len(data) == 23:
                    area = data[12]
                    province_state = data[13]
                    country_region = data[14]
                    last_update = fmt_date(data[15])
                    lat = data[16]
                    long_ = data[17]
                    confirm = int(data[18])
                    dead = int(data[19])
                    heal = int(data[20])
                    active = int(data[21])

                elif len(data) == 12:
                    area = data[1]
                    province_state = data[2]
                    country_region = data[3]
                    last_update = fmt_date(data[4])
                    lat = data[5]
                    long_ = data[6]
                    confirm = int(data[7])
                    dead = int(data[8])
                    heal = int(data[9])
                    active = int(data[10])

                else:
                    continue

                if confirm == 0:
                    heal_rate = '%.2f' % 0.00
                    dead_rate = '%.2f' % 0.00
                else:
                    heal_rate = '%.2f' % ((heal / confirm) * 100)
                    dead_rate = '%.2f' % ((dead / confirm) * 100)

                sql = '''
                    INSERT INTO BASE_00070_NCOV_US(AREA, PROVINCE_STATE, COUNTRY_REGION, 
                                                    LAST_UPDATE, LAT, LONG_, CONFIRM_VALUE, 
                                                    ACTIVE, DEAD_VALUE, HEAL_VALUE, 
                                                    DEAD_RATE, HEAL_RATE, SYNC_NUMBER, SYNC_TIME)
                    VALUES('{0}', '{1}', '{2}',
                        TO_DATE('{3}','YYYY-MM-DD HH24:MI:SS'),
                        '{4}', '{5}', '{6}', '{7}', '{8}', '{9}',
                        '{10}', '{11}', '{12}', SYSDATE)
                '''.format(area, province_state, country_region, 
                    last_update, lat, long_, confirm, active, dead, heal, dead_rate, 
                    heal_rate, max_sn)

                logger.info(sql)

                cur.execute(sql)

            else:
                continue

        # 提交
        conn.commit()

    except Exception as e:
        logger.info(' --- 操作异常 --- ')
        logger.error(str(e))
        conn.rollback()

    finally:
        # 资源关闭
        cur.close()
        conn.close()
        logger.info(' --- 数据库关闭 --- ')


def main():
    try:
        # print(fmt_date('2020-03-22 17:28:17'))
        logger.info(' --- 定时任务开启 ---')
        parse_data()
        
        # # 创建调度器：BlockingScheduler
        # scheduler = BlockingScheduler()
        
        # # 添加任务,时间间隔1小时
        # # scheduler.add_job(parse_data, 'interval', hours=1, id='yq_job')
        # # scheduler.add_job(parse_data, 'interval', seconds=10, id='yq_job')

        # trigger = IntervalTrigger(days=1)
        # scheduler.add_job(parse_data, trigger)

        # scheduler.add_job(func=parse_data,trigger=CronTrigger(hour=8))

        # # # 开启任务
        # scheduler.start()

    except Exception as e:
        logger.info(' --- 定时任务失败 ---')
        logger.error(str(e))


if __name__ == '__main__':
    main()