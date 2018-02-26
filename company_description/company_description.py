# !/usr/bin/env python3
# -*- coding:utf-8*-


import jieba
import jieba.analyse
import pandas as pd
import numpy as np
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
df=loadData('./select_company_name_company_description_.tsv')
df.columns = ['name', 'description']
#去重
df1=df.drop_duplicates('name')
# dfTest=df1.head(10);
dfTest=df1
#载入自定义词典
jieba.load_userdict('./dict.txt')
#载入停词表
jieba.analyse.set_stop_words('./stopwords.txt')
#print(type(dfTest))
try:
    count=1

    for i in dfTest.index:
        chinese_keywords=jieba.analyse.textrank(dfTest.loc[i].values[1],withWeight=True, topK=30,allowPOS=('ns','n', 'vn'))
        # print(count,file=f)
        company_name=dfTest.loc[i].values[0]
        for word in chinese_keywords:
            # print('%s,%s'%(word[0],word[1]),file=f)
            sql = "INSERT INTO company_description_output(company_name,word,weight) VALUES ('%s','%s','%s')"  %(company_name,word[0],word[1]*5)
            cursor.execute(sql)
            db.commit()


except Exception as err:
    print(err)
finally :
    db.close()
    print("Connection closed")
