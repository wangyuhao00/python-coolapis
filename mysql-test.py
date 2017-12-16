#!/usr/bin/python3
import os
import pymysql
import cx_Oracle as cx
from backports import configparser
from pip._vendor.distlib.compat import raw_input


cf = configparser.ConfigParser()
cf.read("config.ini")

def generate_namedtuple(cur):
    from collections import namedtuple
    fieldnames = [d[0].lower() for d in cur.description]
    Record = namedtuple('Record', fieldnames)
    rows = cur.fetchall()
    if not rows:
        return
    else:
        return map(Record._make, rows)


def generate_dicts(cur):
    fieldnames = [d[0].lower() for d in cur.description]
    while True:
        rows = cur.fetchmany()
        if not rows: return
        for row in rows:
            yield dict(zip(fieldnames, row))


def serSLM():
    if __name__ == '__main__':
        log_user = cf.get("mysqldb", "db_user")
        log_pwd = cf.get("mysqldb", "db_pass")
        log_host = cf.get("mysqldb", "db_host")

        bsp_user = cf.get("orcaledb", "db_user")
        bsp_pwd = cf.get("orcaledb", "db_pass")
        bshost = cf.get("orcaledb", "db_host")

        conn = cx.connect(bsp_user+'/'+bsp_pwd+'@'+bshost+'/orcl')
        cursor = conn.cursor()

        num = raw_input("请输入要查询的内容:0、退出  1、查询所有数据库  2、查询对应库里所有的表  3或任意键、根据受理编码查询submit表和failure表   ")
        if num == '0':
            os._exit(0)
        if num == '1':
            cnx = pymysql.connect(log_host, log_user, log_pwd)
            cur = cnx.cursor()
            cur.execute("show databases")
            for r in generate_dicts(cur):
                print(r)
            serSLM()
        if num == '2':
            database = raw_input("请输入数据库名称:   ")
            if database == "":
                database = "dv_db_log"
            cnx = pymysql.connect(log_host, log_user, log_pwd, database)
            cur = cnx.cursor()
            cur.execute("show tables")
            for r in generate_dicts(cur):
                print(r)
            serSLM()
        if num == '3'or num == "":
            slbm = raw_input("请输入29位受理编码:   ")
            if slbm != "":
                ifregion = raw_input("是否通过受理码自动判断地市(0为手动输入,不填写或者任意键为自动判断):   ")
                if ifregion != "0":
                    short_code = slbm[0:9]
                    cursor.execute("select t.region_code from pub_organ t where t.short_code = '" + short_code + "'")
                    row = cursor.fetchone()
                    region = row[0][0:6]
                    cursor.close()
                    conn.close()
                else:
                    region = raw_input("请输入6位所在区划代码(不填写默认为420000):   ")

                if region == "":
                    region = "420000"
                cnx = pymysql.connect(log_host, log_user, log_pwd, "dv_db_log")
                cur = cnx.cursor()

                print("正在查找log_submit_" + region + "表.....")
                cur.execute("select * from log_submit_" + region + " where code = '" + slbm + "'")
                if cur.rowcount == 0:
                    print("log_submit_" + region + "表无数据，开始查询log_submit_failure表......")
                    cur.execute("select * from log_submit_failure  where code = '" + slbm + "'")
                    if cur.rowcount == 0:
                        print("log_submit_failure表无数据，开始查询log_consumer_error表......")
                        cur.execute("select * from log_consumer_error  where message like '%" + slbm + "%'")
                        for r in generate_dicts(cur):
                            print(r)
                        if cur.rowcount == 0:
                            print('库里无此数据')
                            serSLM()
                    else:
                        for r in generate_dicts(cur):
                            print(r)
                        serSLM()
                elif cur.rowcount > 1:
                    print("查询有重复数据,请查看接口调用情况")
                    for r in generate_dicts(cur):
                        print(r)
                    serSLM()
                else:
                    for r in generate_dicts(cur):
                        print(r)
                    serSLM()
            else:
                print("请输入29位受理码.....")
                serSLM()

serSLM()
