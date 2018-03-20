# -*- coding: UTF-8 -*-

# Author 		: 	钱昊达
# Created 		: 	05 March 2018
# Modified 		:
# Version 		: 	1.0

"""
统计job_name词频
"""

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import jieba


def split(line):
    word_list = jieba.cut(line.strip().split("\t")[1])  # 进行中文分词
    ls = []
    for word in word_list:
        # 过滤掉单音节词
        if len(word) > 1:
            ls.append(word)
    return ls

# 去除保存结果中的括号和解决中文编码显示的问题
def combine(line):
    result = ""
    result += line[0] + "\t" + str(line[1])  # 让数字在前，方便统计
    return result


def main(sc):
    text = sc.textFile("E:\cloud_dataset\job_name.csv")
    word_list = text.map(split).collect()  # 保存为列表
    count = sc.parallelize(word_list[0])  # 返回列表中的第一个元素
    results = count.map(lambda w: (w, 1)).reduceByKey(lambda x, y: x + y).map(combine).sortByKey().saveAsTextFile(
        "E:\cloud_dataset\word_frequency.txt")


if __name__ == "__main__":
    conf = SparkConf().setAppName("word frequency")
    conf.setMaster("local")
    sc = SparkContext(conf=conf)
    main(sc)
