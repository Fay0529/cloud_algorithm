# -*- coding: UTF-8 -*-

# Author 		: 	钱昊达
# Created 		: 	3 February 2018
# Modified 		:
# Version 		: 	1.0

"""

处理job_location字段

"""

import pymysql

db=pymysql.connect(host='47.100.163.219',
                    user='root',
                    password='Cloud12345',
                    db='cloud',
                    charset='utf8mb4')


cursor=db.cursor()

sql="select job_id,job_location from job2_0  "

try:
    cursor.execute(sql)
    results=cursor.fetchall()
    numrows = int(cursor.rowcount)
    print(numrows)
    for row in results:
        id=int(row[0])
        job_location=row[1].split('-')
        print(id, job_location[0])
        sql = " update job2_0 set job_city = '%s' where job_id = '%d'" %(job_location[0],id)
        try:
            cursor.execute(sql)
            db.commit()
        except:
            db.rollback()
            print("Error: unable to update")
except Exception as err:
    print(err)

db.close()
print("Connection closed")

