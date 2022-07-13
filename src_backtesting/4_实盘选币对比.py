#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
import numpy  as np
from glob import glob
from datetime import datetime, timedelta
# SettingWithCopyWarning
pd.set_option('mode.chained_assignment', None)

from utils import reader, tools, ind


#==================================================================
#==================================================================
#==================================================================
#==================================================================
def default_handler(df1, df2):
	return df1, df2


def filter_before(df1, df2):
	#"""
	filter_factor = 'AdaptBolling_fl_100'
	# 破下轨不做多
	df1 = df1[~(df1[filter_factor] == -1)]
	# 破上轨不做空
	df2 = df2[~(df2[filter_factor] ==  1)]
	#"""

	return df1, df2


def gen_selected(df, select_coin_num, before_handler, after_handler):
	df1 = df.copy()
	df2 = df.copy()
	# 前置过滤
	df1, df2 = eval(before_handler)(df1, df2)

	# 根据因子对比进行排名
	# 从小到大排序
	df1['排名1'] = df1.groupby('candle_begin_time')['因子'].rank(method='first')
	df1 = df1[(df1['排名1'] <= select_coin_num)].copy()
	df1['方向'] = 1

	# 从大到小排序
	df2['排名2'] = df2.groupby('candle_begin_time')['因子'].rank(method='first', ascending=False)
	df2 = df2[(df2['排名2'] <= select_coin_num)].copy()
	df2['方向'] = -1

	# 后置过滤
	df1, df2 = eval(after_handler)(df1, df2)

	del df2['排名2']
	del df1['排名1']

	# 合并排序结果
	df = pd.concat([df1, df2], ignore_index=True)

	df = df[['candle_begin_time', 'symbol', '方向']]
	df.sort_values(by=['candle_begin_time', '方向'], ascending=[True, False], inplace=True)
	df.reset_index(drop=True, inplace=True)

	return df


def convert_to_filter_cls(all_filter_list):
	results = set()
	for filter_, params in all_filter_list:
		results.add(filter_)
	return list(results)


def convert_to_filter(all_filter_list):
	results = set()
	for filter_, params in all_filter_list:
		results.add('%s_fl_%s' % (filter_, str(params)))
	return list(results)
#==================================================================
#==================================================================
#==================================================================
#==================================================================
stratagy_list = [
	{
		"c_factor":    "c_factor1",  
		"hold_period": "6H",
		"type":        "横截面",
		"factors": [
			('Bias', False, 4, 0, 1.0),('Cci', True, 36, 0, 0.3)
		],
		"filters": [
			('AdaptBolling', 100), ('BBW', [20, 2])
		],
		"filters_handle": {
			"before": 'filter_before',
			'after':  'default_handler',
		},
		"long_weight":       1,
		"short_weight":      1,
		"select_coin_num":   1,
	},
	{
		"c_factor":    "c_factor2",  
		"hold_period": "6H",
		"type":        "横截面",
		"factors": [
			('Bias', False, 4, 0, 1.0),('Cci', True, 36, 0, 0.3)
		],
		"filters": [],
		"filters_handle": {
			"before": 'default_handler',
			'after':  'default_handler',
		},
		"long_weight":       1,
		"short_weight":      1,
		"select_coin_num":   1,
	},
	{
		"c_factor":    "百因子",  
		"hold_period": "6H",
		"type":        "横截面",
		"factors": [
			('Bias', True, 12, 0, 0.5), ('Cci', True, 4, 0, -0.1), ('Cci', True, 8, 0, 0.4), ('Bias', True, 4, 0, 0.3), ('Bias', True, 48, 0, -0.5), ('Cci', True, 30, 0, -0.4), ('Bias', True, 36, 0, 0.1), ('Bias', True, 30, 0, 0.6), ('Cci', True, 96, 0, -0.9), ('Cci', True, 9, 0, -1.0), ('Cci', True, 48, 0, -0.8), ('Bias', True, 72, 0, -0.1), ('Bias', True, 96, 0, 0.8), ('Cci', True, 24, 0, 0.2), ('Bias', True, 3, 0, 0.8), ('Cci', True, 36, 0, -0.6), ('Cci', True, 3, 0, 0.1), ('Bias', True, 60, 0, 0.1), ('Cci', True, 60, 0, -0.6), ('Bias', True, 9, 0, 0.1), ('Bias', True, 6, 0, 1.0), ('Cci', True, 72, 0, -0.1), ('Bias', True, 8, 0, -0.4), ('Cci', True, 12, 0, 0.9), ('Bias', True, 24, 0, 0.1), 
		],
		"filters": [],
		"filters_handle": {
			"before": 'default_handler',
			'after':  'default_handler',
		},
		"long_weight":       1,
		"short_weight":      1,
		"select_coin_num":   1,
	},
	{
		"c_factor":    "lasso",  
		"hold_period": "6H",
		"type":        "纵截面",
		"factors": [
			('Bias', True, 30, 0, -17.12139161127422),('Bias', True, 36, 0, -7.319543605225477),('Bias', True, 4, 0, -0.46805207271163196),('Cci', True, 3, 0, -0.00015315921056340784),('Bias', True, 96, 0, 0.7177159150564656),('Bias', True, 3, 0, 0.6117244602628059),('Cci', True, 24, 0, 0.0002809635974661574),('Cci', True, 4, 0, -2.659624052634533e-05),('Bias', True, 24, 0, 20.29683359255328),('Cci', True, 48, 0, 0.0012869610841340507),('Cci', True, 8, 0, 0.00010777199718734396),('Cci', True, 30, 0, 0.0008542498996673277),('Bias', True, 8, 0, 0.5588303494746705),('Cci', True, 72, 0, 0.0011219536745510285),('Bias', True, 12, 0, 0.07952677978466707),('Bias', True, 72, 0, 0.7797845059481301),('Cci', True, 60, 0, -0.0017813577134868283),('Cci', True, 6, 0, 0.00016532912957839486),('Cci', True, 36, 0, -0.0008257809766224287),('Cci', True, 12, 0, -0.0005635837663922271),('Cci', True, 96, 0, 2.0745292410454156e-05),('Bias', True, 9, 0, -7.339379836799855),('Bias', True, 48, 0, 13.762453913157907),('Cci', True, 9, 0, -0.00015514286805485221),('Bias', True, 60, 0, -7.978598711402175),
		],
		"filters": [],
		"filters_handle": {
			"before": 'default_handler',
			'after':  'default_handler',
		},
		"long_weight":       1,
		"short_weight":      1,
		"select_coin_num":   1,
	},
]


trade_type = 'swap'  
run_time   = datetime.strptime('2021-10-30 03:00:00', "%Y-%m-%d %H:%M:%S")

all_factor_list = []
all_filter_list = []
for strategy in stratagy_list:
	all_factor_list.extend(strategy['factors'])
	all_filter_list.extend(strategy['filters'])

factor_class_list   = tools.convert_to_cls(all_factor_list)
feature_list        = tools.convert_to_feature(all_factor_list)
filter_class_list   = convert_to_filter_cls(all_filter_list)
filter_feature_list = convert_to_filter(all_filter_list)


# ===读取数据
alldata = reader.readhour(trade_type, factor_class_list, filter_class_list=filter_class_list)
# K线时区对齐, 回测数据k线默认使用UTC时区因此也需要兼容下实盘
# 兼容时区
utc_offset = int(time.localtime().tm_gmtoff/60/60)
alldata['candle_begin_time'] = alldata['candle_begin_time'] + pd.Timedelta(hours=utc_offset)
# 因为回测用了df[factor_name] = df[factor_name].shift(1), 
# 当前小时的因子数据是上一个小时的结果。
alldata['candle_begin_time'] -= timedelta(hours=1)
# 删除某些行数据
alldata = alldata[alldata['volume'] > 0]  # 该周期不交易的币种
alldata.dropna(subset=['下个周期_avg_price'], inplace=True)  # 最后几行数据，下个周期_avg_price为空
# 筛选日期范围
alldata = alldata[alldata['candle_begin_time'] < run_time]
alldata = alldata[alldata['candle_begin_time'] > (run_time - timedelta(hours=1500))]
# 数据预处理
alldata = alldata[['candle_begin_time', 'symbol'] + feature_list + filter_feature_list]
alldata = alldata.set_index(['candle_begin_time', 'symbol']).sort_index()
alldata = alldata.replace([np.inf, -np.inf], np.nan)
# 因子空值都用中位数填充, 如果填0可能后面rank排序在第一或者最后
#alldata = alldata.fillna(value=0).reset_index()
alldata[feature_list] = alldata[feature_list].apply(lambda x:x.fillna(x.median()))
alldata = alldata.reset_index()
print('数据处理完毕!!!\n')



select_coin_list = []
for strategy in stratagy_list:
	c_factor        = strategy['c_factor']
	hold_hour       = strategy['hold_period']
	type_ 		    = strategy['type']
	factor_list     = strategy['factors']
	filter_list     = strategy['filters']
	before_handler  = strategy['filters_handle']['before']
	after_handler   = strategy['filters_handle']['after']
	long_weight     = strategy['long_weight'] 
	short_weight    = strategy['short_weight'] 
	select_coin_num = strategy['select_coin_num'] 
	if type_ not in ('横截面', '纵截面'): raise ValueError(f'{type_} 类型不对')

	# ===计算因子
	if type_ == '横截面':
		df = tools.cal_factor_by_cross(alldata.copy(), factor_list)
	else:
		df = tools.cal_factor_by_verical(alldata.copy(), factor_list)
	# ===时间过滤
	df = df[df['candle_begin_time'] >= (run_time - timedelta(hours=int(hold_hour[:-1])))]
	# ===只保留有用字段
	df = df[['candle_begin_time', 'symbol', '因子'] + convert_to_filter(filter_list)]
	# ===选币
	df = gen_selected(df, select_coin_num, before_handler, after_handler)
	# ===处理字段
	#df['offset'] = df['candle_begin_time'].apply(lambda x: x.to_pydatetime().hour%int(hold_hour[:-1]))
	#df['key']    = df['candle_begin_time'].apply(lambda x: f'{c_factor}_{hold_hour}_' + str(x.to_pydatetime().hour%int(hold_hour[:-1])) + 'H')
	df['offset'] = df['candle_begin_time'].apply(lambda x: int(((x.to_pydatetime() - pd.to_datetime('2017-01-01')).total_seconds()/3600)%int(hold_hour[:-1])))
	df['key']    = df['candle_begin_time'].apply(lambda x: f'{c_factor}_{hold_hour}_' + str(int(((x.to_pydatetime() - pd.to_datetime('2017-01-01')).total_seconds()/3600)%int(hold_hour[:-1]))) + 'H')

	select_coin_list.append(df)
# ===合并选币结果
select_coin = pd.concat(select_coin_list)

print(select_coin)





