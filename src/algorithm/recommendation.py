# -*- coding: UTF-8 -*-

# Author 		: 	钱昊达
# Created 		: 	18 March 2018
# Modified 		:
# Version 		: 	1.0

"""

基于内容的推荐单节点版本

"""

from pyspark import SparkContext
from pyspark.sql import SQLContext
import math
import pymysql

'''
职位向量：
job_id 职位号,
salary_mid 工资中值,
company_nature 公司类型,
education_degree 学历,
job_exp 工作经验,
lng 经度,
lat 纬度,
classification 职位分类
'''
class Job_vector:
    def __init__(self,job_id,salary_mid,company_nature,education_degree,job_exp,lng,lat,classification):
        self.job_id=job_id
        self.salary_mid=salary_mid
        self.company_nature=company_nature
        self.education_degree=education_degree
        self.job_exp=job_exp
        self.lng=lng
        self.lat=lat
        self.classification=classification

'''
计算两条职位向量的相似度
'''
def sim(job_vector_1,job_vector_2):
    city_length=math.pow((job_vector_1.lng-job_vector_2.lng),2)+math.pow((job_vector_1.lat-job_vector_2.lat),2)
    sim_city=1-float(math.sqrt(city_length)/100)
    sim_salary_mid=1-float(abs(job_vector_1.salary_mid-job_vector_2.salary_mid)/max(job_vector_1.salary_mid,job_vector_2.salary_mid))
    sim_company_nature=int(job_vector_1.company_nature==job_vector_2.company_nature)
    if job_vector_1.education_degree<job_vector_1.education_degree:
        sim_education_degree=0
    elif job_vector_1.education_degree==job_vector_1.education_degree:
        sim_education_degree = 1
    else:
        sim_education_degree = 0.5
    sim_job_exp=int(job_vector_1.company_nature>=job_vector_2.company_nature)
    return float(0.22*sim_city+0.25*sim_salary_mid+0.18* sim_company_nature+0.21*sim_education_degree+0.13*sim_job_exp)


if __name__ == '__main__':
    sc = SparkContext('local', 'item_based')
    sqlContext = SQLContext(sc)
    items = sc.textFile('E:\cloud_dataset\cloud_job_vector_3.csv').map(lambda line: line.split(',')).cache()
    df = items.toDF(
        ['job_id', 'salary_mid', 'company_nature', 'education_degree', 'job_exp', 'lng', 'lat', 'classification'])
    db = pymysql.connect(host='***',
                         user='***',
                         password='***',
                         db='cloud',
                         charset='utf8mb4')
    cursor = db.cursor()
    sql = "select * from user_record"
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        numrows = int(cursor.rowcount)
        print("rows", numrows)
        for row in results:
            user_id = int(row[0])
            job_id = int(row[1])
            sql_1 = "select * from job_vector_3 where job_id=%s " % (job_id)
            try:
                cursor.execute(sql_1)
                results_1 = cursor.fetchall()
                for row_1 in results_1:
                    job_id = int(row_1[0])
                    salary_mid = int(row_1[1])
                    company_nature = int(row_1[2])
                    education_degree = int(row_1[3])
                    job_exp = float(row_1[4])
                    lng = float(row_1[5])
                    lat = float(row_1[6])
                    classification = int(row_1[7])
                    job_vector_1 = Job_vector(job_id, salary_mid, company_nature, education_degree, job_exp, lng, lat,
                                              classification)
                    #只选取与用户浏览职位相同职位分类的职位
                    df1 = df.filter(df.classification == classification).collect()
                    job_rdd = sc.parallelize(df1)
                    #构造job_id,sim键值对
                    sim_rdd = job_rdd.map(lambda job_vector_2: (job_vector_2[0], sim(job_vector_1,
                                                                                     Job_vector(int(job_vector_2[0]),
                                                                                                int(job_vector_2[1]),
                                                                                                int(job_vector_2[2]),
                                                                                                int(job_vector_2[3]),
                                                                                                float(job_vector_2[4]),
                                                                                                float(job_vector_2[5]),
                                                                                                float(job_vector_2[6]),
                                                                                                int(job_vector_2[7])))))
                    #按照相似度sim降序排序，并去掉与用户浏览记录相同的那条job_vector
                    recommendations = sim_rdd.sortBy(lambda x: x[1], ascending=False).collect()[1:10]
                    for item in recommendations:
                        sql_2="insert into user_recommendation(user_id,job_id) values(%s,%s) "%(user_id,item[0])
                        try:
                            cursor.execute(sql_2)
                            db.commit()
                        except Exception as err:
                            print(err)
                            print("err3")
            except Exception as err:
                print(err)
                print("err2")
    except Exception as err:
        print(err)
        print("err1")
    db.close()
    print("Connection closed")
    sc.stop()
