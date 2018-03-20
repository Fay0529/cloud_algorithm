# -*- coding: UTF-8 -*-

# Author 		: 	钱昊达
# Created 		: 	05 March 2018
# Modified 		:
# Version 		: 	1.0

"""

统计job_name词频

"""

import jieba
import jieba.analyse
import pymysql

#加载停词表
def load_stopwords(path='E:\cloud_dataset\stopwords_1.txt'):
    with open(path,encoding='UTF-8') as f:
        stopwords = filter(lambda x: x, map(lambda x: x.strip(), f.readlines()))
    return list(stopwords)


def form_dict():
    #字典，用于收录词汇和统计词频
    dict={}
    count=0
    # 连接数据库
    db = pymysql.connect(host='***',
                         user='***',
                         password='***',
                         db='***',
                         charset='utf8mb4')
    # 停词表
    #停词表
    stopwords = load_stopwords()
    #加载自定义字典
    jieba.load_userdict("E:\cloud_dataset\job_name_dict.txt")
    jieba.add_word('开发工程师')
    jieba.add_word('高级工程师')
    jieba.add_word('商务经理')
    jieba.add_word('销售经理')
    jieba.add_word('区域经理')
    jieba.add_word('大数据')
    jieba.add_word('云计算')
    jieba.add_word('数据挖掘')
    jieba.add_word('分析')
    cursor = db.cursor()
    sql = sql = "select job_name from job_name "
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        numrows = int(cursor.rowcount)
        print("rows",numrows)
        for row in results:
            job_name = row[0]
            # 分词

            words =jieba.cut(job_name, cut_all=False)
            for item in words:
                if item not in stopwords:
                    if dict.__contains__(item):
                        dict[item]+=1
                    else:
                        dict[item]=1
    except Exception as err:
        print(err)

    #把词频统计写进数据库
    for k,v in dict.items():
        sql_1="insert into word_frequency(word,frequency) values('%s','%s') "%(k,v)
        try:
            cursor.execute(sql_1)
            db.commit()
        except Exception as err:
            print(err)

    # 中断连接
    db.close()
    print("Connection closed")
    for k,v in dict.items():
        print(k,v)


if __name__ == "__main__":
    form_dict()
