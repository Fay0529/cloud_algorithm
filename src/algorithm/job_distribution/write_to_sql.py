# !/usr/bin/env python3
# -*- coding:utf-8*-
# Author 		: 	陈小飞
# Created 		: 	26 February 2018
# Modified 		:
# Version 		: 	1.0

import pandas as pd
import pymysql
#连接数据库
db = pymysql.connect(host='localhost',
                     user='root',
                     password='123456',
                     db='cloud',
                     charset='utf8mb4')
cursor=db.cursor()


# 加载文件函数
def loadData(path):
    df=pd.read_csv(path,sep='\t',header=None)
    return df
df=loadData('./part-r-00000')
dfTest=df
try:

    for i in dfTest.index:
        sql = "INSERT INTO job_distribution(job_city,job_number) VALUES ('%s','%s')"  %(dfTest.loc[i].values[0],dfTest.loc[i].values[1])
        cursor.execute(sql)
        db.commit()


except Exception as err:
    print(err)
finally :
    db.close()
    print("Connection closed")
