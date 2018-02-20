# -*- coding: UTF-8 -*-

import jieba
import jieba.analyse
import pymysql

#加载停词表
def load_stopwords(path='***stopwords.txt'):
    with open(path,encoding='UTF-8') as f:
        stopwords = filter(lambda x: x, map(lambda x: x.strip(), f.readlines()))
    return list(stopwords)

def calculate(base,index):
    # 连接数据库
    db = pymysql.connect(host='***',
                         user='***',
                         password='***',
                         db='***',
                         charset='***')
    # 停词表
    stopwords = load_stopwords()
    en_stopwords=load_stopwords(***ENstopwords.txt')
    cursor = db.cursor()
    sql = sql = "select * from job_description limit %d,%d" %(base,index)

    count = 0

    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        numrows = int(cursor.rowcount)
        print(numrows)
        for row in results:
            job_id = int(row[0])
            job_description = row[1]
            # 获取中文关键词
            chinese_keywords = jieba.analyse.textrank(job_description, topK=30, withWeight=True,
                                                      allowPOS=('ns', 'n', 'vn', 'v'))
            for item in chinese_keywords:
                # 筛选中文关键词
                if item[0] not in stopwords:
                    sql = "INSERT INTO job_description_output(job_id,word,weight) VALUES ('%d','%s','%s')" % (
                    job_id, item[0], item[1] * 5)
                    try:
                        cursor.execute(sql)
                        db.commit()
                        print(job_id, count)
                        count = count + 1
                    except Exception as err:
                        db.rollback()
                        print(err)

        keywords = jieba.cut(job_description)
        en_keywords=set()
        #去重
        for item in keywords:
            en_keywords.add(item)
        for item in en_keywords:
            # 筛选出英语关键词，权重暂时自己设置了
            if (item.encode('UTF-8').isalpha() and item not in en_stopwords):
                sql = "INSERT INTO job_description_output(job_id,word,weight) VALUES ('%d','%s','%s')" % (
                    job_id, item, 4.5)
                try:
                    cursor.execute(sql)
                    db.commit()
                    # print(job_id,item)
                except Exception as err:
                    db.rollback()
                    print(err)
    except Exception as err:
        print(err)
    # 中断连接
    db.close()
    print("Connection closed")

jieba.add_word("Machine Learning")
for x in range(42):
    print(x)
    #每次提交一部分，便于网络中断后的修复                           
    calculate(x*1000+1,1000)

