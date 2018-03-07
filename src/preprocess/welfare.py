# -*- coding:utf-8*-

# Author 		: 	钱昊达
# Created 		: 	16 February 2018
# Modified 		: 	20 February 2018
# Version 		: 	1.1

"""

处理welfare字段

"""


import re
import pymysql


db=pymysql.connect(host='***',
                    user='***',
                    password='***',
                    db='***',
                    charset='utf8mb4')

cursor=db.cursor()

sql="select job_id,welfare from job2_0 "

count=0

try:
    cursor.execute(sql)
    results=cursor.fetchall()
    numrows = int(cursor.rowcount)
    print(numrows)
    for row in results:
        job_id=int(row[0])
        welfares=row[1].split('\r')
        for item in welfares:
            if item is not "":
                s=item.replace(' ','')
                q=s.replace(',','')
                try:
                    sql_1 = "insert into job_welfare(job_id, welfare) values('%s','%s') " %(job_id, q)
                    cursor.execute(sql_1)
                    db.commit()
                    print(job_id, q)
                except Exception as err:
                    print(err)
                count=count+1
        print(count)
except Exception as err:
    print(err)

db.close()
print("Connection closed")

