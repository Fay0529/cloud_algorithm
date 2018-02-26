# -*- coding: UTF-8 -*-

# Author 		: 	陈小飞
# Created 		: 	25 February 2018
# Modified 		:
# Version 		: 	1.0
import jieba
import jieba.analyse
import pandas as pd
# 加载文件函数
def loadData(path):
    df=pd.read_csv(path,sep='\t',header=None)
    return df
df=loadData('/home/fay/workspace/python/cloud_algorithm/job_five_factors/select_job_name_from_job2_0.tsv')
dfTest=df
jieba.load_userdict('/home/fay/workspace/python/cloud_algorithm/job_five_factors/job_name_dict.txt') # file_name 为文件类对象或自定义词典的路径
try:
    f = open("job_name_cut.txt", "w")
    for i in dfTest.index:
        chinese_words=jieba.cut(dfTest.loc[i].values[0])
        for word in chinese_words:
            print(word,end=' ',file=f)
        print('',file=f)



except Exception as err:
    print(err)
finally :

    print("Connection closed")
