# -*- coding: UTF-8 -*-

import jieba
import jieba.analyse
import pymysql

#加载停词表
def load_stopwords(path='***'):
    with open(path,encoding='UTF-8') as f:
        stopwords = filter(lambda x: x, map(lambda x: x.strip(), f.readlines()))
    return list(stopwords)

#连接数据库
db = pymysql.connect(host='***',
                     user='***',
                     password='***',
                     db='***',
                     charset='utf8mb4')
#停词表
stopwords=load_stopwords()


cursor=db.cursor()
sql="select * from job_description1"

try:
    cursor.execute(sql)
    results=cursor.fetchall()
    numrows = int(cursor.rowcount)
    print(numrows)
    for row in results:
        job_id=int(row[0])
        job_description=row[1]
        #获取中文关键词
        chinese_keywords = jieba.analyse.textrank(job_description, topK=30, withWeight=True, allowPOS=('ns', 'n', 'vn', 'v'))
        for item in chinese_keywords:
            #筛选中文关键词
            if item[0] not in stopwords:
                sql = "INSERT INTO job_description_output(job_id,word,weight) VALUES ('%d','%s','%s')"  %(job_id,item[0],item[1]*5)
                try:
                 cursor.execute(sql)
                 db.commit()
                 print(job_id,item[0],item[1])
                except Exception as err:
                 db.rollback()
                 print(err)

    keywords = jieba.cut(job_description)
    for item in keywords:
        #筛选出英语关键词，权重暂时自己设置了
        if (item.encode('UTF-8').isalpha()):
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

#中断连接
db.close()
print("Connection closed")


