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
socket.setdefaulttimeout(30)  # 设置socket层的超时时间为20秒


class GetEpidemicData(object):
    """docstring for GetEpidemicData"""
    def __init__(self, np, url):
        self.conn = cx.connect(np)
        self.cur = self.conn.cursor()
        self.url = url
        self.logger = make_log('us_epidemic','github_epidemic')
        
        # 创建调度器：BlockingScheduler
        self.scheduler = BlockingScheduler()
        # 代理
        self.proxy='zzy:root123@192.168.66.199:80'
        self.proxy_handler = {
                'http': 'http://' + proxy,
                'https': 'https://' + proxy
            }


    # 无代理
    def get_data():
        firefox_headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}
        # response = urlopen(url=url, timeout=60)
        request =  Request(self.url, headers=firefox_headers)
        response = urlopen(request)
        # 字节转换字符串
        content = response.read().decode().replace(u'\xf1', u'').replace(u"'", u"")
        return content


    # 有代理
    def get_data_proxy(self):
        firefox_headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}
        request =  Request(self.url, headers=firefox_headers)
        # 创建一个ProxyHandler对象
        proxy_support=urllib.request.ProxyHandler(self.proxy_handler)
        # 创建一个opener对象
        opener = urllib.request.build_opener(proxy_support)
        # 给request装载opener
        urllib.request.install_opener(opener)
        response = urlopen(request)
        # 字节转换字符串
        content = response.read().decode().replace(u'\xf1', u'').replace(u"'", u"")
        return content


    def parse_data(self):
        self.logger.info(' --- 数据库连接 --- ')
        try:
            # 获取当前最晚同步次数
            s_sql = '''SELECT MAX(SYNC_NUMBER) FROM BASE_00070_NCOV_US'''
            self.cur.execute(s_sql)
            max_sn = cur.fetchone()[0]
            if max_sn == None:
                max_sn = 1
            else:
                max_sn += 1

            us_data = get_data_proxy(url)

            data_list = us_data.split('"')

            for i in range(len(data_list)):
                data = data_list[i].split(',')
                if 'US' in data:
                    if len(data) == 23:
                        area = data[12]
                        province_state = data[13]
                        country_region = data[14]
                        sys_date = data[15]
                        confirm = int(data[18])
                        dead = int(data[19])
                        heal = int(data[20])
                        active = int(data[21])

                    elif len(data) == 12:
                        area = data[1]
                        province_state = data[2]
                        country_region = data[3]
                        sys_date = data[4]
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
                    # print(area, province_state, country_region, sys_date, confirm, dead, heal, active, heal_rate, dead_rate)

                    sql = '''
                        INSERT INTO BASE_00070_NCOV_US(AREA, PROVINCE_STATE, COUNTRY_REGION, CONFIRM_VALUE, ACTIVE, DEAD_VALUE, HEAL_VALUE, DEAD_RATE, HEAL_RATE, SYNC_NUMBER, SYNC_TIME)
                        VALUES('{0}', '{1}', '{2}', {3}, {4}, {5}, {6}, {7}, {8}, {9}, TO_DATE('{10}','YYYY-MM-DD HH24:MI:SS'))
                    '''.format(area, province_state, country_region, confirm, active, dead, heal, dead_rate, heal_rate, max_sn, sys_date)

                    self.logger.info(sql)

                    self.cur.execute(sql)

                else:
                    continue

            # 提交
            self.conn.commit()

        except Exception as e:
            self.logger.info(' --- 操作异常 --- ')
            self.logger.error(str(e))
            self.conn.rollback()

        finally:
            # 资源关闭
            self.cur.close()
            self.conn.close()
            self.logger.info(' --- 数据库关闭 --- ')


    def create_scheduler(self):
        try:
            parse_data()

            self.logger.info(' --- 定时任务开启 ---')

            self.scheduler.add_job(func=get_data_proxy, trigger=IntervalTrigger(seconds=10, id='yq_job'))
            

            # 添加任务,时间间隔1小时
            self.scheduler.add_job(func=parse_data, trigger=IntervalTrigger(seconds=10, id='yq_job'))
            # self.scheduler.add_job(func=parse_data,trigger=CronTrigger(hour=8))

            # 添加监听
            self.scheduler.add_listener(scheduler_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
            
            # 开启任务
            self.scheduler.start()

        except Exception as e:
            self.logger.info(' --- 定时任务失败 ---')
            self.logger.error(str(e))


    def remove_scheduler(self, id):
         scheduler.remove_job('interval_task')


    def scheduler_listener(self, event):
        print(event)
        if event.exception:
            print ('任务出错了！！！！！！')
        else:
            print ('任务正常常运行...')


if __name__ == '__main__':
    np = 'flxuser/flxuser@192.168.158.219:1521/bfcecdw'
    # 拼接url
    cur_date = (date.today() + timedelta(days = -1)).strftime('%m-%d-%Y')
    url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/%s.csv' % cur_date
    print(url)
    cl = GetEpidemicData(np, url)
    cl.create_scheduler()