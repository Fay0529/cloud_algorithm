# -*- coding: UTF-8 -*-

###
_author_='钱昊达'
预处理job_location字段，析出city_name
###
import pymysql

db=pymysql.connect(host='***',
                    user='***',
                    password='***',
                    db='***',
                    charset='***')


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
        sql = " UPDATE job2_0 SET job_city = '%s' WHERE job_id = '%d'" %(job_location[0],id)
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







