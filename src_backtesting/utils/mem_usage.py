#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np
from joblib import Parallel, delayed
from time import time
from tqdm import tqdm

def show_mem_df(df):
	return df.memory_usage().sum() / 1024**2


def reduce_mem_series(col):
	numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64', 'object']
	col_type = col.dtypes
	if col_type in numerics:
		c_min = col.min()
		c_max = col.max()
		if str(col_type)[:3] == 'int':
			if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
				col = col.astype(np.int8)
			elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
				col = col.astype(np.int16)
			elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
				col = col.astype(np.int32)
			elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
				col = col.astype(np.int64)
		elif str(col_type)[:5] == 'float':
			if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
				col = col.astype(np.float16)
			elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
				col = col.astype(np.float32)
			else:
				col = col.astype(np.float64)
		else:
			num_unique_values = len(col.unique())
			num_total_values  = len(col)
			rate = num_unique_values/num_total_values
			if rate <0.5:
				col = col.astype('category')
	return col


def reduce_mem_usage_org(df, feature_list, njobs=16, verbose=True):
	start_mem = show_mem_df(df)  
	if verbose:
		print("Memory usage of the dataframe before converted is :", start_mem, "MB")

	results = Parallel(n_jobs=njobs)(delayed(reduce_mem_series)(df[f]) for f in feature_list)
	for col in results:
		df[col.name] = col

	end_mem = show_mem_df(df)
	if verbose:
		print("Memory usage of the dataframe after converted is :", end_mem, "MB")
	if verbose: 
		print('Mem. usage decreased to {:5.2f} Mb ({:.1f}% reduction)'.format(end_mem, 100 * (start_mem - end_mem) / start_mem))
	return df


def reduce_mem_usage(df, feature_list, njobs=8, verbose=True):
   starttime = time()
   start_mem = show_mem_df(df)
   if verbose:
      print("Memory usage of the dataframe before converted is :", start_mem, "MB")

   results = Parallel(n_jobs=njobs)(delayed(reduce_mem_series)(df[f]) for f in tqdm(feature_list, desc='cal col in df'))
   # head_columns = ['candle_begin_time', 'symbol', 'ret_next', 'AdaptBolling_fl_100', 'ZH_涨跌幅_fl_4', 'ZH_震幅_fl_4']
   # _df = df[head_columns]
   _df = df.copy()
   for col in tqdm(results, desc='join col to df'):
      _df[col.name] = col

   end_mem = show_mem_df(_df)
   if verbose:
      print("Memory usage of the dataframe after converted is :", end_mem, "MB")
   if verbose:
      print('Mem. usage decreased to {:5.2f} Mb ({:.1f}% reduction)'.format(end_mem, 100 * (start_mem - end_mem) / start_mem))
   print('完成', '用时：', round(time() - starttime, 6), 's')
   # print(df)
   # print(_df)
   del df
   return _df

