# !/usr/bin/env python3
# -*- coding:utf-8*-
# Author 		: 	陈小飞
# Created 		: 	25 February 2018
# Modified 		:
# Version 		: 	1.0

"""
利用正则表达式对job_experience字段进行处理
"""
import pandas as pd
import numpy as np
import pymysql
import re
def filter(str):
    m=re.match(r'(\d*)-*(\d*)',str)
    if m.group(1) != '':
        low=int(m.group(1))
    else:
        low=0
    if m.group(2)!= '':
        high=int(m.group(2))
    else:
        high=low
    average=(high+low)/2
    return low,high,average
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
df=loadData('./select_job_id_job_experience_from_job2_0.tsv')
df.columns = ['job_id', 'exp']
dfTest=df
try:
    for i in dfTest.index:
        l=filter(dfTest.loc[i].values[1])
        job_id=dfTest.loc[i].values[0]
        sql = "INSERT INTO job_five_factors(job_id,job_exp_low,job_exp_high,job_exp_average) VALUES ('%s','%s','%s','%s')"  %(job_id,l[0],l[1],l[2])
        cursor.execute(sql)
        db.commit()


except Exception as err:
    print(err)
finally :
    db.close()
    print("Connection closed")
