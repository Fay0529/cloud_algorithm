# -*- coding: UTF-8 -*-

# Author 		: 	陈小飞
# Created 		: 	05 March 2018
# Modified 		:
# Version 		: 	1.0

"""

回归分析职位薪酬影响因素

"""
APP_NAME = "Spark Regression"
from pyspark import SparkConf, SparkContext
import numpy as np
from pyspark.mllib.regression import LabeledPoint
import os, tempfile
from pyspark.mllib.regression import LinearRegressionWithSGD
f=open('logout.txt','w')
def get_mapping(rdd,idx):
    """
    将类型特征表示成二维形式，我们将特征值映射到二元向量中的非0位置
    """
    return rdd.map(lambda fields: fields[idx]).distinct().zipWithIndex().collectAsMap()
def extract_features(record,cat_len,mappings):
    cat_vec = np.zeros(cat_len)
    step = 0
    for i,raw_feature in enumerate(record[1:3]):
        dict_code = mappings[i]
        index = dict_code[raw_feature]
        cat_vec[index+step] = 1
        step = step+len(dict_code)

    num_vec = np.array([float(raw_feature) for raw_feature in record[3:6]])
    return np.concatenate((cat_vec, num_vec))

def extract_label(record):
    return float(record[0])

def main(sc):
    path='/user/hadoop/regression/dataforclass1.tsv'
    raw_data=sc.textFile(path)
    num_data=raw_data.count()
    records=raw_data.map(lambda x:x.split('\t'))
    records.cache()
    mappings=[get_mapping(records,i) for i in range(1,3)] #对类型变量的列（第1-2列）应用映射函数
    print('类别特征打编码字典：',mappings,file=f)
    cat_len=sum(map(len,[i for i in mappings])) #类别特征的个数
    num_len=len(records.first()[3:6]) #数值特征的个数
    total_len=num_len+cat_len #所有特征个数
    print('类别特征个数：%d' % cat_len,file=f)
    print('数值特征的个数:%d' %num_len,file=f)
    print('所有特征的个数:%d' %total_len,file=f)
    #下面对数据进行特征提取
    data = records.map(lambda point: LabeledPoint(extract_label(point),extract_features(point,cat_len,mappings)))
    first_point = data.first()
    print('标签:' + str(first_point.label),file=f)
    print('对类别特征进行独热编码之后的特征向量: \n' + str(first_point.features),file=f)
    print('对类别特征进行独热编码之后的特征向量长度:' + str(len(first_point.features)),file=f)
    linear_model=LinearRegressionWithSGD.train(data,iterations=10,step=0.1,intercept=False)
    len_1=len(mappings[0])
    print('len1:%s  len2:%s'%(len_1,cat_len-len_1),file=f)
    l=linear_model.weights
    w1=sum(l[0:len_1])/len_1
    w2=sum(l[len_1:cat_len])/(cat_len-len_1)
    w3=l[-3]
    w4=(l[-2]+l[-1])/2
    print('特征权重：',linear_model.weights,file=f)
    print('四元素权重:[%s\n,%s\n,%s\n,%s\n]'%(w1,w2,w3,w4),file=f)
    path = tempfile.mkdtemp()
    linear_model.save(sc,path)
    f.close()

if __name__ == "__main__":
    conf = SparkConf().setAppName(APP_NAME )
    conf.setMaster("local")
    sc = SparkContext(conf=conf)
    main(sc)
