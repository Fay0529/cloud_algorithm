# !/usr/bin/env python3
# -*- coding:utf-8*-
import multiprocessing
from gensim.models import Word2Vec
from gensim.models.word2vec import LineSentence

# inp为输入语料
inp = '/home/fay/workspace/python/cloud_algorithm/job_five_factors/job_name_cut.txt'
# outp1 为输出模型
outp1 = 'job_name.model'
# outp2为原始c版本word2vec的vector格式的模型
outp2 = 'job_name.vector'
model = Word2Vec(LineSentence(inp), sample=0.00001,size=100, window=5,negative=1, min_count=5,workers=multiprocessing.cpu_count())
model.build_vocab(LineSentence(inp), update=True, progress_per=10000,keep_raw_vocab=True,)
model.save(outp1)
model.wv.save_word2vec_format(outp2, binary=False)
