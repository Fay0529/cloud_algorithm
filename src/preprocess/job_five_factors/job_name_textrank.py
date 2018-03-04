# -*- coding: UTF-8 -*-

# Author 		: 	陈小飞
# Created 		: 	28 February 2018
# Modified 		:
# Version 		: 	1.0
"""
利用jieba对job_name进行关键词统计
"""

import jieba
import jieba.analyse

# 加载文件函数

file_object=open('/home/fay/workspace/python/cloud_algorithm/src/preprocess/job_five_factors/select_job_name_from_job2_0_backup.tsv')


try:
    f = open("job_name_textrank.txt", "w")
    file_context = file_object.read()

    chinese_words=chinese_keywords=jieba.analyse.textrank(file_context,withWeight=True, topK=100)
    for word in chinese_words:
        print(word,end=' ',file=f)
    print('',file=f)



except Exception as err:
    print(err)
finally :

    print("Connection closed")
