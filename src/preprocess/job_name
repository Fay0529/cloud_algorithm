# -*- coding: UTF-8 -*-

# Author 		: 	钱昊达
# Created 		: 	22 February 2018
# Modified 		:   25 February 2018
# Version 		: 	1.2

"""

初步处理job_name字段

"""

import re
import pymysql

count_1=0
count_2=0
count_3=0

db=pymysql.connect(host='47.100.163.219',
                    user='root',
                    password='Cloud12345',
                    db='cloud',
                    charset='utf8mb4')

cursor=db.cursor()

sql="select job_id,job_name from job2_0  "

try:
    cursor.execute(sql)
    results=cursor.fetchall()
    numrows = int(cursor.rowcount)
    print(numrows)
    for row in results:
        job_id=int(row[0])
        job_name_init=row[1].replace(' ','')
        job_name = job_name_init.replace('+', '')
        if re.match(r'(\(|\（).*(\)|\）)+.*',job_name):
            #print(job_id, job_name)
            #job_name_split=re.split('[)）]',job_name)
            job_name_final = job_name_split[0]
            count_1=count_1+1
        elif re.match('.+(\(|\（).*(\)|\）)+.*',job_name):
            #print(job_id, job_name)
            job_name_split = re.split('[(（]', job_name)
            job_name_final=job_name_split[0]
            count_2 = count_2 + 1
        else:
            job_name_final=job_name
            count_3=count_3+1
        sql_1 = "update job2_0 set job_name='%s' where job_id='%s' " %(job_name_final,job_id)
        try:
            cursor.execute(sql_1)
            db.commit()
            print(job_id)
        except Exception as err:
            print(err)
except Exception as err:
    print(err)

print(count_1,count_2,count_3)
db.close()
print("Connection closed")
