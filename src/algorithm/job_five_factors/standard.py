# -*- coding: UTF-8 -*-

# Author 		: 	陈小飞
# Created 		: 	05 March 2018
# Modified 		:
# Version 		: 	1.0

"""

标准化数据

"""
import pymysql
import pandas as pd
import numpy as np
db = pymysql.connect(host='localhost',
                     user='root',
                     password='fay123',
                     db='cloud',
                     charset='utf8mb4')
cursor=db.cursor()
#连接数据库
# 加载文件函数
def loadData(path):
    df=pd.read_csv(path,sep='\t',header=None)
    return df
df=loadData('/home/faychen/workspace/python/cloud_algorithm/src/algorithm/job_five_factors/cloud_job_vector_1_t.tsv')
# df=df.head(50)
# all_columns = list(df.columns.values)
# #the first four columns don't need to be normalized
# all_columns = all_columns[4:len(all_columns)]
# for column in all_columns:
#     df[column] = (df[column]-df[column].min()) / (df[column].max()-df[column].min())
zscore=lambda x:(x-x.mean())/(x.std())
transformed=df[[4,5,6,7]].transform(zscore)
for i in df.index:
    # print(df.loc[i].values[7])
    sql="update job_vector_1 set job_exp=%s,lng=%s,lat=%s,salary_mid=%s where job_id=%s"%(transformed.loc[i].values[0],transformed.loc[i].values[1],transformed.loc[i].values[2],transformed.loc[i].values[3],df.loc[i].values[0])
    cursor.execute(sql)
    db.commit()
