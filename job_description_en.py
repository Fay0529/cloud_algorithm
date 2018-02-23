# -*- coding: UTF-8 -*-

###
_author_='钱昊达'
计算职位需求中出现的英文词汇(几乎都是it术语)的权重
采用算法是Textrank
###

import math
import pymysql

#job_id总数
total_job_id_num=0
#word_id,job_id
dict_0={}
#word_id,word
dict_1={}
#word,count(word)
dict_2={}
#job_id,count(words in job_id)
dict_3={}
#记录某job_id里的word的weight的最大值
dict_4={}
#word_id,weight
dict_5={}

#连接数据库
db = pymysql.connect(host='***',
                     user='***',
                     password='***',
                     db='***',
                     charset='***')
cursor = db.cursor()

sql = "select word_id,job_id,word from job_description_output " \
      "where weight='4.5'"
try:
    cursor.execute(sql)
    results = cursor.fetchall()
    numrows = int(cursor.rowcount)
    count=0
    for row in results:
        dict_0[int(row[0])]=int(row[1])
        dict_1[int(row[0])]=row[2]
        count=count+1
except Exception as err:
        print(err)
print(count)

count_1=0
sql_1="select word,count(job_id) from job_description_output " \
      "where weight='4.5' "\
      "group by word"
try:
    cursor.execute(sql_1)
    results_1 = cursor.fetchall()
    numrows_1 = int(cursor.rowcount)
    count_1=0
    for row in results_1:
        dict_2[row[0]]=int(row[1])
        count_1=count_1+1
except Exception as err:
    print(err)
print(count_1)

count_2=0
sql_2="select job_id,count(word) from job_description_output " \
      "where weight='4.5' " \
      "group by job_id"
try:
    cursor.execute(sql_2)
    results_2 = cursor.fetchall()
    numrows_2 = int(cursor.rowcount)
    count_2=0
    for row in results_2:
        dict_3[int(row[0])]=int(row[1])
        dict_4[int(row[0])]=float(0)
        count_2=count_2+1
except Exception as err:
    print(err)
print(count_2)

sql_3="select count(distinct job_id) from job_description_output "
try:
    cursor.execute(sql_3)
    results_3 = cursor.fetchall()
    numrows_3 = int(cursor.rowcount)
    for row in results_3:
        total_job_id_num=row[0]
        print(total_job_id_num)
except Exception as err:
    print(err)

#计算初步的tf_idf
for k,v in dict_1.items():
    job_id=dict_0[k]
    job_id_num=dict_2[v]
    word_num=dict_3[job_id]
    tf=float(1/word_num)
    idf=math.log(total_job_id_num/job_id_num)
    tf_idf=tf*idf
    dict_5[k]=float(tf_idf)

#更新dict_4
for k,v in dict_0.items():
    #dict_4[v]=dict_4[v]+dict_5[k]
    dict_4[v]=max(dict_4[v],dict_5[k])

#重新计算dict_5
for k,v in dict_0.items():
   dict_5[k]=dict_5[k]/dict_4[v]
   print(k,dict_5[k])

#计算新的tf_idf
for k,v in dict_5.items():
    sql_4 = "update job_description_output set weight='%d' where word_id='%d'" %(v*5,k)
    try:
        cursor.execute(sql_4)
        db.commit()
        pass
    except Exception as err:
        print(err)

# 中断连接
db.close()
print("Connection closed")
