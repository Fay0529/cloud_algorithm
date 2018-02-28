# !/usr/bin/env python3
# -*- coding:utf-8*-
# Author 		: 	陈小飞
# Created 		: 	24 February 2018
# Modified 		:
# Version 		: 	1.0
"""
对训练模型word2vec进行简单测试
"""
from gensim.models import Word2Vec

import jieba
model = Word2Vec.load('job_name.model')
model['系统分析员']
l=list(jieba.cut('Java软件工程师/架构师系统分析员'))
l1=list(jieba.cut('数据挖掘工程师'))
l2=list(jieba.cut('客户经理'))
r1=model.n_similarity(l,l1)
r2=model.n_similarity(l1,l2)
r3=model.n_similarity(l,l2)
print(r1)
print(r2)
print(r3)
