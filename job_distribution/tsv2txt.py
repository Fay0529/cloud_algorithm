# !/usr/bin/env python3
# -*- coding:utf-8*-
import pandas as pd
# 加载文件函数
def loadData(path):
    df=pd.read_csv(path,sep='\t',header=None)
    return df
df=loadData('/home/fay/workspace/python/cloud_algorithm/job_distribution/select_job_city_from_job2_0.tsv')
def Tsv2Txt(tsv='',txt=''):
    f=open(txt,'w')
    df=loadData(tsv)
    for index, row in df.iterrows():   # 获取每行的index、row
        for col_name in df.columns:
            print(row[col_name],file=f)# 把结果返回给data
Tsv2Txt(tsv='/home/fay/workspace/python/cloud_algorithm/job_distribution/select_job_city_from_job2_0.tsv',txt='job_city.txt')
