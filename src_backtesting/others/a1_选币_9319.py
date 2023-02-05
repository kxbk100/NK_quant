#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import time
import glob
import itertools
import pandas as pd
import numpy  as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows',  1000)  # 最多显示数据的行数
# setcopywarning
pd.set_option('mode.chained_assignment', None)
# UserWarning
import warnings
warnings.filterwarnings("ignore")
# FutureWarning
from warnings import simplefilter
simplefilter(action='ignore', category=FutureWarning)
from config import src_path
import sys
sys.path.append(src_path)

from utils import reader, tools, ind


#==================================================================
#==================================================================
#==================================================================
#==================================================================
# 前置过滤
def filter_before(df1, df2):
	# filter_factor = 'BBW_fl_[20, 2]'
	# df1 = df1[(df1[filter_factor] > 0)]

	"""
	filter_factor = 'AdaptBolling_fl_100'
	# 破下轨不做多
	df1 = df1[~(df1[filter_factor] == -1)]
	# 破上轨不做空
	df2 = df2[~(df2[filter_factor] ==  1)]

	filter_factor = 'ZH_震幅_fl_4'
	df1 = df1[(abs(df1[filter_factor]) <= 0.5)]
	df2 = df2[(abs(df2[filter_factor]) <= 0.5)]

	filter_factor = 'ZH_涨跌幅_fl_4'
	df1 = df1[(df1[filter_factor] <=  0.4)]
	df2 = df2[(df2[filter_factor] >= -0.4)]
	"""

	return df1, df2


# 后置过滤
def filter_after(df1, df2):
	# 资金费率为负不做空
	#df2 = df2[df2['fundingRate']>=0]

	return df1, df2


def gen_selected(df, select_coin_num):
	df1 = df.copy()
	df2 = df.copy()
	# 前置过滤
	df1, df2 = filter_before(df1, df2)

	# 根据因子对比进行排名
	# 从小到大排序
	df1['排名1'] = df1.groupby('candle_begin_time')['因子'].rank(method='first')
	df1 = df1[(df1['排名1'] <= select_coin_num)].copy()
	df1['方向'] = 1

	# 从大到小排序
	df2['排名2'] = df2.groupby('candle_begin_time')['因子'].rank(method='first', ascending=False)
	df2 = df2[(df2['排名2'] <= select_coin_num)].copy()
	df2['方向'] = -1

	del df2['排名2']
	del df1['排名1']
	# 后置过滤
	df1, df2 = filter_after(df1, df2)

	# 合并排序结果
	df = pd.concat([df1, df2], ignore_index=True)

	df = df[header_columns + ['方向']]
	df.sort_values(by=['candle_begin_time', '方向'], ascending=[True, False], inplace=True)
	df.reset_index(drop=True, inplace=True)

	return df

#==================================================================
#==================================================================
#==================================================================
#==================================================================


start_date = '2021-01-27'
end_date   = '2022-01-27'

header_columns  = ['candle_begin_time', 'symbol', 'ret_next']
select_coin_num = 1
c_rate 			= 6/10000
trade_type 		= 'swap'
hold_hour  		= '6H'    
filter_list     = []

factor_list = [
    ('Dbcd', True, 48, 0, 0.6), ('Dbcd', True, 24, 0, -0.6), ('VixBw', True, 3, 0, 1.0),
    ('Dbcd', True, 72, 0, -1.0), ('Dbcd', True, 60, 0, 0.3), ('Dbcd', True, 3, 0, -1.0),
    ('VixBw', True, 72, 0, 1.0), ('Dbcd', True, 9, 0, 1.0), ('Dbcd', True, 6, 0, 0.6),
    ('VixBw', True, 48, 0, 1.0), ('Dbcd', True, 8, 0, -0.8), ('Dbcd', True, 30, 0, 0.8),
    ('VixBw', True, 9, 0, 0.9), ('VixBw', True, 8, 0, 1.0), ('VixBw', True, 60, 0, -1.0),
    ('Dbcd', True, 96, 0, 1.0), ('VixBw', True, 96, 0, 0.4), ('VixBw', True, 4, 0, 0.3),
    ('VixBw', True, 6, 0, 0.6), ('Dbcd', True, 4, 0, 1.0), ('Dbcd', True, 36, 0, -0.5),
    ('VixBw', True, 24, 0, -1.0), ('Dbcd', True, 12, 0, 1.0), ('VixBw', True, 36, 0, -1.0),
    ('VixBw', True, 30, 0, 0.9), ('VixBw', True, 12, 0, 1.0),
]



factor_class_list = tools.convert_to_cls(factor_list)
filter_class_list = tools.convert_to_filter_cls(filter_list)
feature_list      = tools.convert_to_feature(factor_list)

# ===读取数据
df = reader.readall(trade_type, hold_hour, factor_class_list, filter_class_list=filter_class_list, date_range=(start_date, end_date))
df = reader.feature_shift(df, feature_list + filter_list)

# 删除某些行数据
df = df[df['volume'] > 0]  # 该周期不交易的币种
df.dropna(subset=['下个周期_avg_price'], inplace=True)  # 最后几行数据，下个周期_avg_price为空
# 筛选日期范围
df = df[df['candle_begin_time'] >= pd.to_datetime(start_date)]
df = df[df['candle_begin_time'] <= pd.to_datetime(end_date)]
# ===数据预处理
df['ret_next'] = df['下个周期_avg_price'] / df['avg_price'] - 1
df = df[header_columns + feature_list + filter_list]
df = df.set_index(['candle_begin_time', 'symbol']).sort_index()
df = df.replace([np.inf, -np.inf], np.nan)
# 因子空值都用中位数填充, 如果填0可能后面rank排序在第一或者最后
#df = df.fillna(value=0).reset_index()
df[feature_list] = df[feature_list].apply(lambda x:x.fillna(x.median()))
df = df.reset_index()
print('数据处理完毕!!!\n')

'''
# ===整合资金费率
fundingrate_data = reader.read_fundingrate()
df = pd.merge(df, 
	fundingrate_data[['candle_begin_time', 'symbol', 'fundingRate']], 
	on=['candle_begin_time', 'symbol'], how="left")
df['fundingRate'].fillna(value=0, inplace=True)
print('整合资金费率完毕!!!\n')
'''

# ===计算因子
# 横截面
df = tools.cal_factor_by_cross(df, factor_list)
# 纵截面
#df = tools.cal_factor_by_verical(df, factor_list)
# ===选币
select_coin = gen_selected(df, select_coin_num)
# ===计算offset
#select_coin['offset'] = select_coin['candle_begin_time'].apply(lambda x: x.to_pydatetime().hour%int(hold_hour[:-1]))
select_coin['offset'] = select_coin['candle_begin_time'].apply(lambda x: int(((x.to_pydatetime() - pd.to_datetime('2017-01-01')).total_seconds()/3600)%int(hold_hour[:-1])))


temp = pd.DataFrame()
# ===显示所有offset资金曲线
for offset, g_df in select_coin.groupby('offset'):
	# print(offset)
	g_df.sort_values(by='candle_begin_time', inplace=True)
	g_df.reset_index(drop=True, inplace=True)
	# ===计算涨跌幅
	select_c = tools.evaluate(g_df, c_rate, select_coin_num)	
	# ===计算资金曲线
	select_c['资金曲线'] = (select_c['本周期多空涨跌幅'] + 1).cumprod()
	select_c['offset'] = offset
	rtn, select_c = ind.cal_ind(select_c)
	# print(rtn)
	temp = temp.append(rtn, ignore_index=True)
	ax = plt.subplot(int(hold_hour[:-1]), 1, offset + 1)
	ax.plot(select_c['candle_begin_time'], select_c['资金曲线'])

print(temp.to_markdown())
plt.gcf().autofmt_xdate()
plt.show()
# exit()

# """
# ===计算涨跌幅
all_select_df = tools.evaluate(select_coin, c_rate, select_coin_num)
# ===合并资金曲线
all_select_df['本周期多空涨跌幅'] = all_select_df['本周期多空涨跌幅']/int(hold_hour[:-1])
all_select_df['资金曲线'] = (all_select_df['本周期多空涨跌幅'] + 1).cumprod()
rtn, select_c = ind.cal_ind(all_select_df)
temp = temp.append(rtn)
print('所有offset平均"年化收益/回撤比" ', temp['年化收益/回撤比'][:-1].mean())
# print('所有offset平均"年化收益/回撤比(合成)" ', temp['年化收益/回撤比'].iloc[-1])
print(temp.to_markdown())
# print(rtn)
print('\n')
tools.plot(select_c, mdd_std=0.2)
# """







