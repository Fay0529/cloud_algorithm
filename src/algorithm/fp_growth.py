# -*- coding: UTF-8 -*-

# Author 		: 	钱昊达
# Created 		: 	01 March 2018
# Modified 		:
# Version 		: 	1.0

"""

使用FP-Gworth算法找出(地点，工资)频繁项集

"""

from pyspark import SparkContext
from pyspark.sql import SQLContext
from  pyspark.mllib.fpm import FPGrowth

sc = SparkContext('local','example')
sqlContext = SQLContext(sc)
city_salary_rdd = sc.textFile('E:\cloud_dataset\city_salary.csv').map(lambda line: line.split(','))
rdd = sc.parallelize(city_salary_rdd, 2)
model = FPGrowth.train(rdd, 0.005, 2)
sorted(model.freqItemsets().collect())
#df=city_salary_rdd.toDF(['Employee_ID','Employee_name'])
#df.show()
