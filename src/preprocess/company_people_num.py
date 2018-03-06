# -*- coding: UTF-8 -*-

# Author 		: 	钱昊达
# Created 		: 	5 February 2018
# Modified 		:
# Version 		: 	1.0

"""

处理company_people_num字段

"""

import re
import pymysql

db=pymysql.connect(host='***',
                    user='***',
                    password='***',
                    db='***',
                    charset='utf8mb4')


cursor=db.cursor()

sql="select id,company_people_num from job2_0 "
count=0
try:
    cursor.execute(sql)
    results=cursor.fetchall()
    numrows = int(cursor.rowcount)
    print(numrows)
    for row in results:
       id=int(row[0])
       company_people_num=row[1]
       if company_people_num is "":
           count=count+1
           pass
       elif re.match(r'少于\d{0,9}人',company_people_num):
           #count=count+1
           company_people_num_low = 1
           company_people_num_high=int(re.sub("\D", "", company_people_num))
           #print(id, company_people_num_high)
       elif re.match(r'\d{0,9}-\d{0,9}人',company_people_num):
           #count = count + 1
           num_set = re.split('[人|-]', company_people_num)
           company_people_num_low = int(num_set[0])
           company_people_num_high = int(num_set[1])
           #print(id, company_people_num_low,company_people_num_high)
       elif re.match(r'\d{0,9}人以上', company_people_num):
           #count = count + 1
           company_people_num_low = int(re.sub("\D", "", company_people_num))
           company_people_num_high = 100000
           #print(id, company_people_num_low)
       else:
           print("格式错误")
           continue
       sql_1="update job2_0 set company_people_num_low = '%s' ,company_people_num_high = '%s' where id = '%s'" %(company_people_num_low,company_people_num_high,id)
       try:
           cursor.execute(sql_1)
           db.commit()
           count=count+1
           print(count)
       except Exception as err:
           print(err)
except Exception as err:
    print(err)
print(count)
db.close()
print("Connection closed")
