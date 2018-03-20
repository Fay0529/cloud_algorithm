# -*- coding: UTF-8 -*-

# Author 		:   陈小飞
# Created 		: 	05 March 2018
# Modified 		:
# Version 		: 	1.0

"""

统计salary分布

"""

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext



def split(line):
    ls = []
    # 过滤掉单音节词
    tmp=int(line)
    tmp=tmp*12
    if tmp<2000:
        ls.append('2万以下')
    elif tmp<3000:
        ls.append('2~3万')
    elif tmp<40000:
        ls.append('3~4万')
    elif tmp<50000:
        ls.append('4~5万')
    elif tmp<60000:
        ls.append('5~6万')
    elif tmp<80000:
        ls.append('6~8万')
    elif tmp<100000:
        ls.append('8~10万')
    elif tmp<150000:
        ls.append('10~15万')
    elif tmp<200000:
        ls.append('15~20万')
    elif tmp<300000:
        ls.append('20~30万')
    elif tmp<400000:
        ls.append('30~40万')
    elif tmp<500000:
        ls.append('40~50万')
    elif tmp<600000:
        ls.append('50~60万')
    elif tmp<800000:
        ls.append('60~80万')
    elif tmp<1000000:
        ls.append('80~100万')
    else:
        ls.append('100万以上')
    return ls

def main(sc):
    text = sc.textFile("file:///home/faychen/Templates/select_salary_mid_from_job_vector.tsv")
    count = text.flatMap(split)  # 保存为列表
    results = count.map(lambda w: (w, 1)).reduceByKey(lambda x, y: x + y).sortByKey().saveAsTextFile(
        "file:///home/faychen/Templates/salaryCountResult1")


if __name__ == "__main__":
    conf = SparkConf().setAppName("Salary Count")
    conf.setMaster("local")
    sc = SparkContext(conf=conf)
    main(sc)
