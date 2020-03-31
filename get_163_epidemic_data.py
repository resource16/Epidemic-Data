# _*_ conding:utf8 _*_
# author: Ziyuan

import json
import datetime
import cx_Oracle as cx
from urllib.request import Request, urlopen
from apscheduler.schedulers.blocking import BlockingScheduler


def get_data(url):
    firefox_headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}
    request =  Request(url, headers=firefox_headers)
    response = urlopen(request)
    # 字节转换字符串
    content = response.read().decode()
    # 字符串转换Json
    data = json.loads(content)
    if data['msg'] == '成功':
        n = 0
        area_data = data['data']['areaTree']
        for i in range(len(area_data)):
            country = area_data[i]['name']
            if country == '中国':
                china_data = area_data[i]['children']
                # china_data.append(area_data[i])
            #     # n = i 
            if country == '圣马丁岛':
                area_data[i]['name'] = '法属圣马丁'

            if country == '波斯尼亚':
                n = i
        area_data.remove(area_data[n])
        return area_data, china_data


def insert_data(i_data, tag, date, num):
    area = i_data['name']
    confirm = i_data['total']['confirm']
    suspect = i_data['total']['suspect']
    add_value = i_data['today']['confirm']
    heal = i_data['total']['heal']
    dead = i_data['total']['dead']
    heal_rate = '%.2f' % ((heal / confirm) * 100)
    dead_rate = '%.2f' % ((dead / confirm) * 100)
    if confirm == None:
        confirm = 0
    if suspect == None:
        suspect = 0
    if add_value == None:
        add_value = 0
    if heal == None:
        heal = 0
    if dead == None:
        dead = 0
    if confirm == 0:
        heal_rate = 0
        dead_rate = 0
    print(area, '---', confirm, '---', suspect, '---', add_value, '---', heal, '---', dead)

    sql = '''
        INSERT INTO BASE_00070_NCOV(AREA, CONFIRM_VALUE, TYPE, ADD_VALUE, DEAD_VALUE, SUSPECT_VALUE, HEAL_VALUE, DEAD_RATE, HEAL_RATE, SYNC_NUMBER, SYNC_TIME)
        VALUES('{0}', {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, TO_DATE('{10}','YYYY-MM-DD HH24:MI:SS'))
    '''.format(area, confirm, tag, add_value, dead, suspect, heal, dead_rate, heal_rate, num, date)

    return sql
        

def parse_data():
    conn = cx.connect('flxuser/flxuser@192.168.158.219:1521/bfcecdw')
    cur = conn.cursor()
    total_data = get_data('https://c.m.163.com/ug/api/wuhan/app/data/list-total')
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

    print(date, ' --- 同步时间')
    print(max_sn, ' --- 同步次数')
    print(len(china_data), ' --- 国内数据')
    print(len(international_data), ' --- 国际数据')
    print(len(china_data) + len(international_data), ' --- 总数据')

    # 插入国内数据    
    for i in range(len(china_data)):
        p_sql = insert_data(china_data[i], 0, date, max_sn)
        # print(p_sql)
        cur.execute(p_sql)

        # 提交
        conn.commit()

    # 插入国际数据
    for i in range(len(international_data)):
        c_sql = insert_data(international_data[i], 1, date, max_sn)
        # print(c_sql)
        cur.execute(c_sql)
        
        # 提交
        conn.commit()

    # 资源关闭
    cur.close()

    conn.close()
    
    
def main():
    # # 创建调度器：BlockingScheduler
    # scheduler = BlockingScheduler()
    
    # # 添加任务,时间间隔1小时
    # scheduler.add_job(execute_sql, 'interval', hours=1, id='yq_job')
    # # scheduler.add_job(execute_sql, 'interval', seconds=10, id='yq_job')
    
    # # 开启任务
    # scheduler.start()
    
    execute_sql()


if __name__ == '__main__':
    main()