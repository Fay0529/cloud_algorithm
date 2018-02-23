# -*- coding: UTF-8 -*-

###
_author_='钱昊达'
预处理salary字段
###

import pymysql
import re

db=pymysql.connect(host='47.100.163.219',
                    user='root',
                    password='Cloud12345',
                    db='cloud',
                    charset='utf8mb4')


cursor=db.cursor()

sql="select job_id,salary from job2_0  where salary like '%千/月%'"

try:
    cursor.execute(sql)
    results=cursor.fetchall()
    numrows = int(cursor.rowcount)
    print(numrows)
    for row in results:
        id=int(row[0])
        salary=row[1]
        salary1=re.split('[千|-]',salary)
        salary_low=int(float(salary1[0])*1000)
        salary_high = int(float(salary1[1])*1000)
        sql = " UPDATE job2_0 SET salary_low = '%s',salary_high='%s'WHERE job_id = '%d'" % (salary_low,salary_high,id)
        try:
            cursor.execute(sql)
            db.commit()
            print(id,salary_low,salary_high)
        except:
            db.rollback()
            print("Error: unable to update")
except Exception as err:
    print(err)

db.close()
print("Connection closed")







