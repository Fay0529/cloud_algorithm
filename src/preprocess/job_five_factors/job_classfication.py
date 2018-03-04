# -*- coding: UTF-8 -*-

# Author 		: 	陈小飞
# Created 		: 	28 February 2018
# Modified 		:
# Version 		: 	1.0
"""
对job进行分类
"""

import pandas as pd
import pymysql

# 加载文件函数
def loadData(path):
    df=pd.read_csv(path,sep='\t',header=None)
    return df
dfTest=pd.read_csv('/home/faychen/workspace/python/cloud_algorithm/src/preprocess/job_five_factors/job_classfication',sep=',',header=None)
df=loadData('/home/faychen/workspace/python/cloud_algorithm/src/preprocess/job_five_factors/select_job_id_job_name_from_job2_0.tsv')
data=df
# 连接数据库
db = pymysql.connect(host='localhost',
                     user='root',
                     password='fay123',
                     db='cloud',
                     charset='utf8mb4')
cursor = db.cursor()
try:


    for indexes in data.index:
        job_id=data.loc[indexes].values[0]
        job_name =data.loc[indexes].values[1]
        flag=0
        for i in dfTest.index:
            word=dfTest.loc[i].values[0]
            job_class=int(dfTest.loc[i].values[1])
            if word in job_name.lower():
                flag=1
                break
        if flag == 0:
            job_class=16
        sql="insert into job_classify(job_id,job_class,job_name) values('%s',%d,'%s')" %(job_id,job_class,job_name)
        cursor.execute(sql)
        db.commit()

except Exception as err:
    print(err)
finally :

    print("Connection closed")
