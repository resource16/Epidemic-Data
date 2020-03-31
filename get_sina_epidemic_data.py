# -*- coding: utf-8 -*- 
# author: Ziyuan


import sys
import os
import importlib
import json
import datetime
import requests
import cx_Oracle as cx
import urllib
from urllib.request import Request, urlopen
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from loger import make_log

importlib.reload(sys)
os.environ['NLS_LANG'] = 'Simplified Chinese_CHINA.ZHS16GBK'

logger = make_log('epidemic','sina_epidemic')

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
    response = urlopen(request)
    # 字节转换字符串
    content = response.read().decode()
    # 字符串转换Json
    data = json.loads(content)['data']
    china_data = data['list']
    international_data = data['worldlist']
    international_data[0]['conadd'] = data['add_daily']['addcon']
    return china_data, international_data


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
    content = response.read().decode()
    # 字符串转换Json
    data = json.loads(content)['data']
    # print(data)
    china_data = data['list']
    international_data = data['worldlist']
    international_data[0]['conadd'] = data['add_daily']['addcon']
    return china_data, international_data


def parse_data():
    conn = cx.connect('flxuser/flxuser@192.168.158.219:1521/bfcecdw')
    cur = conn.cursor()
    logger.info(' --- 数据库连接 --- ')
    try:
        total_data = get_data_proxy('https://interface.sina.cn/news/wap/fymap2020_data.d.json')
        date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        china_data = total_data[0]
        international_data = total_data[1]

        # 获取当前最晚同步次数
        s_sql = '''SELECT MAX(SYNC_NUMBER) FROM BASE_00070_NCOV'''
        cur.execute(s_sql)
        max_sn = cur.fetchone()[0]
        if max_sn == None:
            max_sn = 1
        else:
            max_sn += 1

        cd_sum = len(china_data)
        itd_sum  = len(international_data)
        citd_sum  = cd_sum + itd_sum

        logger.info('同步次数 --- %s' % max_sn)
        logger.info('同步时间 --- %s' % date)
        logger.info('国内数据 --- %s' % cd_sum)
        logger.info('国际数据 --- %s' % itd_sum)
        logger.info('总    数 --- %s' % citd_sum)

        # 插入国内数据    
        for i in range(len(china_data)):
            p_sql = insert_data(china_data[i], 0, date, max_sn)
            # print(p_sql)
            cur.execute(p_sql)


        # 插入国际数据
        for i in range(len(international_data)):
            c_sql = insert_data(international_data[i], 1, date, max_sn)
            # print(c_sql)
            cur.execute(c_sql)

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


def insert_data(data, tag, date, num):
    try:
        area = data['name'].strip()
        confirm = data['value'].strip()
        suspect = data['susNum'].strip()
        add_value = data['conadd'].strip()
        heal =data['cureNum'].strip()
        dead = data['deathNum'].strip()

        if confirm == "待公布":
            confirm = 0
        else:
            confirm = int(confirm)

        if suspect == "待公布":
            suspect = 0
        else:
            suspect = int(suspect)

        if add_value == "待公布":
            add_value = 0
        else:
            add_value = int(add_value)

        if heal == "待公布":
            heal = 0
        else:
            heal = int(heal)

        if dead == "待公布":
            dead = 0
        else:
            dead = int(dead)

        if confirm == 0:
            heal_rate = 0.00
            dead_rate = 0.00
        else:
            heal_rate = '%.2f' % ((heal / confirm) * 100)
            dead_rate = '%.2f' % ((dead / confirm) * 100)

        res = {
            '疫情地区': area,
            '确诊人数': confirm,
            '国内/国外': tag,
            '新增人数': add_value,
            '死亡人数': dead,
            '疑似人数': suspect,
            '治愈人数': heal,
            '死亡比例': dead_rate,
            '治愈比例': heal_rate,
            '同步次数': num,
            '同步时间': date
        }
        # logger.info(res)

        sql = '''
            INSERT INTO BASE_00070_NCOV(AREA, CONFIRM_VALUE, TYPE, ADD_VALUE, DEAD_VALUE, SUSPECT_VALUE, HEAL_VALUE, DEAD_RATE, HEAL_RATE, SYNC_NUMBER, SYNC_TIME)
            VALUES('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', TO_DATE('{10}','YYYY-MM-DD HH24:MI:SS'))
        '''.format(area, confirm, tag, add_value, dead, suspect, heal, dead_rate, heal_rate, num, date)

        logger.info(sql)

        return sql
    except Exception as e:
        logger.error(str(e))


def main():
    try:
        # parse_data()

        logger.info(' --- 定时任务开启 ---')
        # 创建调度器：BlockingScheduler
        scheduler = BlockingScheduler()

        # 添加任务,时间间隔1小时
        # scheduler.add_job(parse_data, 'interval', hours=1, id='yq_job')
        # scheduler.add_job(parse_data, 'interval', seconds=10, id='yq_job')

        trigger = IntervalTrigger(hours=1)
        scheduler.add_job(parse_data, trigger)
        
        # 开启任务
        scheduler.start()

    except Exception as e:
        logger.info(' --- 定时任务失败 ---')
        logger.error(str(e))
        
    


if __name__ == '__main__':
    main()