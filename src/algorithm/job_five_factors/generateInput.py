# !/usr/bin/env python3
# -*- coding:utf-8*-
# Author 		: 	陈小飞
# Created 		: 	18 February 2018
# Modified 		:
# Version 		: 	1.0


"""
从数据库中抽取数据生成文件
"""
import pymysql
#连接数据库
db = pymysql.connect(host='localhost',
                     user='root',
                     password='fay123',
                     db='cloud',
                     charset='utf8mb4')
cursor=db.cursor()
try:
    for i in range(24,25):
        sql="select salary_mid,company_nature,education_degree,job_exp,lng,lat from job_vector_1 where job_class=%s; "% i
        f=open("/home/faychen/bigdata/input/class%s"%i,"w")
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results :
            print("%s %s %s %s %s %s"%(row[0],row[1],row[2],row[3],row[4],row[5]),file=f)
        f.close()
except Exception as err:
    print(err)
finally :
    db.close()
    print("Connection closed")
