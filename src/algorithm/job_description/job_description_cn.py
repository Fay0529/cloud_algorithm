# -*- coding: UTF-8 -*-

# Author 		: 	钱昊达
# Created 		: 	17 February 2018
# Modified 		:
# Version 		: 	1.0

"""
对job_description进行分词、滤词
使用textrank算法计算job_description中中文关键词的权重
"""

import jieba
import jieba.analyse
import pymysql

#加载停词表
def load_stopwords(path='E:\cloud_dataset\stopwords.txt'):
    with open(path,encoding='UTF-8') as f:
        stopwords = filter(lambda x: x, map(lambda x: x.strip(), f.readlines()))
    return list(stopwords)

def calculate(base,index):
    # 连接数据库
    db = pymysql.connect(host='***',
                         user='***',
                         password='***',
                         db='***',
                         charset='utf8mb4')
    # 停词表
    #中文表
    cn_stopwords = load_stopwords()
    #英文停词表
    en_stopwords=load_stopwords('E:\cloud_dataset\ENstopwords.txt')
    cursor = db.cursor()
    sql = sql = "select * from job_description_input limit %d,%d" %(base,index)
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        numrows = int(cursor.rowcount)
        print("rows",numrows)
        for row in results:
            job_id = int(row[0])
            description = row[1]
            # 获取中文关键词
            chinese_keywords = jieba.analyse.textrank(description, topK=30, withWeight=True,allowPOS=('ns', 'n', 'vn', 'v'))
            for item in chinese_keywords:
                # 筛选中文关键词
                if item[0] not in cn_stopwords:
                    sql = "INSERT INTO job_description_output(job_id,word,weight) VALUES ('%d','%s','%s')" % (job_id, item[0], item[1] * 5)
                    try:
                        cursor.execute(sql)
                        db.commit()
                    except Exception as err:
                        db.rollback()
                        print(err)
            print(job_id,"cn")
            keywords = jieba.cut(description)
            en_keywords=set()
            for item in keywords:
                if (item.encode('UTF-8').isalpha() and (item not in en_stopwords)):
                    item_lower_case=item.lower()
                    en_keywords.add(item_lower_case)
            for item in en_keywords:
                # 筛选出英语关键词，权重暂时自己设置了
                if (item.encode('UTF-8').isalpha() ):
                    sql = "INSERT INTO job_description_output(job_id,word,weight) VALUES ('%d','%s','%s')" % (
                        job_id, item, 4.5)
                try:
                    cursor.execute(sql)
                    db.commit()
                except Exception as err:
                    db.rollback()
                    print(err)
            print(job_id, "en")
    except Exception as err:
        print(err)
    # 中断连接
    db.close()
    print("Connection closed")

jieba.add_word("Machine Learning")
jieba.add_word("java ee")
jieba.add_word("JAVA EE")
jieba.add_word("Java EE")
jieba.add_word("Java ee")


for x in range(42):
    print(x)
    calculate(x*1000,1000)
